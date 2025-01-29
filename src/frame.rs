use crate::frame::Frame::Simple;
use anyhow::{anyhow, Context};
use bytes::{Buf, Bytes};
use memchr::memchr;
use std::io::Cursor;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Stream ended early")]
    Incomplete,
    #[error("Unsupported frame")]
    UnsupportedFrameType,
    #[error(transparent)]
    UnexpectedError(#[from] anyhow::Error),
}

#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

impl Frame {
    fn simple(mut buf: Cursor<&[u8]>) -> Result<Frame, Error> {
        let line = read_line(&mut buf)?.to_vec();

        let str = String::from_utf8(line).context("protocol error; invalid frame format")?;

        Ok(Simple(str))
    }
}

pub(crate) fn parse(mut buf: Cursor<&[u8]>) -> Result<Frame, Error> {
    match get_u8(&mut buf)? {
        b'+' => Frame::simple(buf),
        b'-' => todo!("Simple Errors"),
        b':' => todo!("Integers"),
        b'$' => todo!("Bulk Strings"),
        b'*' => todo!("Arrays"),
        _ => Err(Error::UnsupportedFrameType),
    }
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

pub(crate) fn read_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let buffer = *src.get_ref();

    let Some(cr_pos) = memchr(b'\r', &buffer[start..]) else {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\r\\n not found."
        )));
    };

    let expected_lf_pos = start + cr_pos + 1;

    if buffer.len() <= expected_lf_pos || buffer[expected_lf_pos] != b'\n' {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\n not found or in wrong position."
        )));
    }

    if let Some(_) = memchr(b'\n', &buffer[start..expected_lf_pos]) {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\n found in wrong position."
        )));
    }

    src.set_position(expected_lf_pos as u64);

    Ok(&buffer[start..expected_lf_pos - 1])
}

#[cfg(test)]
mod tests {
    use crate::frame::{parse, Frame};
    use claims::{assert_err, assert_ok};
    use proptest::prelude::{any, Strategy};
    use proptest::proptest;
    use std::io::Cursor;

    proptest! {
        #[test]
        fn simple_string_frame_valid(frame_bytes in valid_simple_string_strategy()) {
            // Arrange
            let buf = Cursor::new(&frame_bytes[..]);

            // Act
            let frame = parse(buf);

            // Assert
            assert_ok!(&frame);
            if let Ok(Frame::Simple(content)) = frame {
                let expected_content = String::from_utf8(frame_bytes[1..frame_bytes.len()-2].to_vec()).unwrap();
                assert_eq!(content, expected_content);
            } else {
                panic!("Expected Frame::Simple variant")
            }
        }

        #[test]
        fn read_valid_line_from_any_position((prefix, line, suffix) in valid_line_with_prefix_and_suffix_strategy()) {
            // Arrange
            // Build test data: [prefix]+[line]\r\n[suffix]
            let mut data = prefix.clone();
            data.extend_from_slice(&line);
            data.extend_from_slice(&suffix);

            let mut cursor = Cursor::new(data.as_slice());
            cursor.set_position((prefix.len()) as u64);

            // Act
            let frame = parse(cursor);

            // Assert

            assert_ok!(&frame);

            if let Ok(Frame::Simple(content)) = frame {
                let expected_content = String::from_utf8(line[1..line.len()-2].to_vec()).unwrap();
                assert_eq!(content, expected_content);
            } else {
                panic!("Expected Frame::Simple variant")
            }
        }
    }

    #[test]
    fn simple_string_frame_cursor_not_in_start_valid() {
        // Arrange
        let buf = b"unimportant+CONTENT\r\nasdasd";
        let mut buf = Cursor::new(buf.as_slice());
        buf.set_position(11);

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Simple(content)) = frame {
            assert_eq!(content, "CONTENT");
        } else {
            panic!("Expected Frame::Simple variant")
        }
    }

    #[test]
    fn simple_string_frame_invalid() {
        // Arrange
        let buf = b"+Hello World\n\r";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_err!(&frame);
    }

    #[test]
    fn simple_string_frame_contains_cr_invalid() {
        // Arrange
        let buf = b"+Hello \r World\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_err!(&frame);
    }

    #[test]
    fn simple_string_frame_contains_lf_invalid() {
        // Arrange
        let buf = b"+Hello \n World\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_err!(&frame);
    }

    // ------------------------------------------------
    // ------------------ Strategies ------------------
    // ------------------------------------------------

    fn valid_simple_string_strategy() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            any::<char>().prop_filter("Exclude '\\r' and '\\n'", |c| *c != '\r' && *c != '\n'),
            0..341,
        )
        .prop_map(|chars| {
            let content: String = chars.into_iter().collect();
            let mut frame_str = String::from("+");
            frame_str.push_str(&content);
            frame_str.push_str("\r\n");
            frame_str.into_bytes()
        })
    }

    fn valid_line_with_prefix_and_suffix_strategy() -> impl Strategy<Value = (Vec<u8>, Vec<u8>, Vec<u8>)> {
        (
            proptest::collection::vec(any::<u8>(), 0..43),
            valid_simple_string_strategy(),
            proptest::collection::vec(any::<u8>(), 0..51),
        )
    }
}
