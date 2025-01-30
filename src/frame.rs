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
    fn simple(line: &[u8]) -> Result<Frame, Error> {
        let str = String::from_utf8(line.to_vec())
            .context("protocol error; invalid simple string format")?;

        Ok(Frame::Simple(str))
    }

    fn error(line: &[u8]) -> Result<Frame, Error> {
        let str = String::from_utf8(line.to_vec())
            .context("protocol error; invalid simple error format")?;

        Ok(Frame::Error(str))
    }

    fn integer(line: &[u8]) -> Result<Frame, Error> {
        let s = std::str::from_utf8(line).context("protocol error; invalid integer format")?;

        s.parse::<i64>()
            .map(Frame::Integer)
            .map_err(|_| Error::UnexpectedError(anyhow!("protocol error; invalid integer")))
    }
}

pub fn parse(mut buf: Cursor<&[u8]>) -> Result<Frame, Error> {
    let frame_type = get_u8(&mut buf)?;
    let line = read_line(&mut buf)?;
    match frame_type {
        b'+' => Frame::simple(line),
        b'-' => Frame::error(line),
        b':' => Frame::integer(line),
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

fn read_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
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

    if memchr(b'\n', &buffer[start..expected_lf_pos]).is_some() {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\n found in wrong position."
        )));
    }

    src.set_position(expected_lf_pos as u64);

    Ok(&buffer[start..expected_lf_pos - 1])
}

#[cfg(test)]
mod tests {
    use crate::frame::{parse, read_line, Frame};
    use claims::{assert_err, assert_ok};
    use proptest::prelude::{any, Strategy};
    use proptest::proptest;
    use std::io::Cursor;

    proptest! {

        #[test]
        fn read_line_valid_from_any_position((prefix, content, suffix) in valid_line_with_prefix_and_suffix_strategy()) {
            // Arrange
            // [prefix][content]\r\n[suffix]
            let mut data = prefix.clone();
            data.extend_from_slice(&content);
            data.extend_from_slice(&suffix);

            let mut cursor = Cursor::new(data.as_slice());
            cursor.set_position(prefix.len() as u64);

            // Act
            let line = read_line(&mut cursor);

            // Assert
            assert_ok!(&line);
            let line_wo_crlf = &content[..content.len() - 2];
            assert_eq!(line.unwrap(), line_wo_crlf);
        }

        #[test]
        fn simple_string_frame_valid(frame_bytes in valid_simple_string_strategy()) {
            // Arrange
            let line = frame_bytes.as_slice();

            // Act
            let frame = Frame::simple(line);

            // Assert
            assert_ok!(&frame);
            if let Ok(Frame::Simple(content)) = frame {
                let expected_content = String::from_utf8(frame_bytes.to_vec()).unwrap();
                assert_eq!(content, expected_content);
            } else {
                panic!("Expected Frame::Simple variant")
            }
        }
        
        #[test]
        fn simple_error_frame_valid(frame_bytes in valid_simple_error_strategy()) {
            // Arrange
            let line = frame_bytes.as_slice();

            // Act
            let frame = Frame::error(line);

            // Assert
            assert_ok!(&frame);
            if let Ok(Frame::Error(content)) = frame {
                let expected_content = String::from_utf8(frame_bytes.to_vec()).unwrap();
                assert_eq!(content, expected_content);
            } else {
                panic!("Expected Frame::Simple variant")
            }
        }
    }

    #[test]
    fn read_line_crlf_order_invalid() {
        // Arrange
        let buf = b"unimportant\n\r";
        let mut buf = Cursor::new(buf.as_slice());

        // Act
        let line = read_line(&mut buf);

        // Assert
        assert_err!(&line);
    }

    #[test]
    fn read_line_content_contains_cr_invalid() {
        // Arrange
        let buf = b"unimportant\runimportant\r\n";
        let mut buf = Cursor::new(buf.as_slice());

        // Act
        let line = read_line(&mut buf);

        // Assert
        assert_err!(&line);
    }

    #[test]
    fn read_line_content_contains_lf_invalid() {
        // Arrange
        let buf = b"unimportant\nunimportant\r\n";
        let mut buf = Cursor::new(buf.as_slice());

        // Act
        let line = read_line(&mut buf);

        // Assert
        assert_err!(&line);
    }

    #[test]
    fn parse_simple_string_frame_valid() {
        // Arrange
        let buf = b"+Hello World\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Simple(content)) = frame {
            assert_eq!(content, "Hello World");
        } else {
            panic!("Expected Frame::Simple variant");
        }
    }
    
    #[test]
    fn parse_simple_error_frame_valid() {
        // Arrange
        let buf = b"-ERR unknown command 'asdf'\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Error(content)) = frame {
            assert_eq!(content, "ERR unknown command 'asdf'");
        } else {
            panic!("Expected Frame::Error variant");
        }
    }

    #[test]
    fn parse_integer_frame_valid() {
        // Arrange
        let buf = b":123\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, 123);
        } else {
            panic!("Expected Frame::Error variant");
        }
    }

    #[test]
    fn parse_integer_frame_with_positive_signal_valid() {
        // Arrange
        let buf = b":+123\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, 123);
        } else {
            panic!("Expected Frame::Integer variant");
        }
    }

    #[test]
    fn parse_integer_frame_negative_valid() {
        // Arrange
        let buf = b":-123\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, -123);
        } else {
            panic!("Expected Frame::Integer variant");
        }
    }

    #[test]
    fn parse_integer_frame_max_valid() {
        // Arrange
        let buf = b":9223372036854775807\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, i64::MAX);
        } else {
            panic!("Expected Frame::Integer variant");
        }
    }

    #[test]
    fn parse_integer_frame_min_valid() {
        // Arrange
        let buf = b":-9223372036854775808\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, i64::MIN);
        } else {
            panic!("Expected Frame::Integer variant");
        }
    }

    #[test]
    fn parse_integer_frame_invalid() {
        // Arrange
        let buf = b":12a3\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_err!(&frame);
    }

    #[test]
    fn parse_integer_frame_large_invalid() {
        // Arrange
        // i64::MAX + 1
        let buf = b":9223372036854775808\r\n";
        let buf = Cursor::new(buf.as_slice());

        // Act
        let frame = parse(buf);

        // Assert
        assert_err!(&frame);
    }

    // ------------------------------------------------
    // ------------------ Strategies ------------------
    // ------------------------------------------------

    fn valid_string_content_strategy() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            any::<char>().prop_filter("Exclude '\\r' and '\\n'", |c| *c != '\r' && *c != '\n'),
            0..341,
        )
        .prop_map(|chars| {
            let mut content: String = chars.into_iter().collect();
            content.push_str("\r\n");
            content.into_bytes()
        })
    }

    fn valid_simple_string_strategy() -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(
            any::<char>().prop_filter("Exclude '\\r' and '\\n'", |c| *c != '\r' && *c != '\n'),
            0..341,
        )
        .prop_map(|chars| chars.into_iter().collect::<String>().into_bytes())
    }

    fn valid_simple_error_strategy() -> impl Strategy<Value = Vec<u8>> {
        // follows same protocol as simple string
        valid_simple_string_strategy()
    }

    fn valid_line_with_prefix_and_suffix_strategy(
    ) -> impl Strategy<Value = (Vec<u8>, Vec<u8>, Vec<u8>)> {
        (
            proptest::collection::vec(any::<u8>(), 0..43),
            valid_string_content_strategy(),
            proptest::collection::vec(any::<u8>(), 0..51),
        )
    }
}
