use anyhow::{anyhow, Context};
use btoi::btoi;
use bytes::{Buf, Bytes};
use memchr::memchr;
use std::io::Cursor;

pub type Result<T> = std::result::Result<T, Error>;

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
    fn simple(line: &[u8]) -> std::result::Result<Self, Error> {
        let str = String::from_utf8(line.to_vec())
            .context("protocol error; invalid simple string format")?;

        Ok(Self::Simple(str))
    }

    fn error(line: &[u8]) -> Result<Self> {
        let str = String::from_utf8(line.to_vec())
            .context("protocol error; invalid simple error format")?;

        Ok(Self::Error(str))
    }

    fn integer(line: &[u8]) -> Result<Self> {
        btoi::<i64>(line)
            .map(Frame::Integer)
            .map_err(|_| Error::UnexpectedError(anyhow!("protocol error; invalid integer format")))
    }
}

pub fn parse(buff: &mut Cursor<&[u8]>) -> Result<Frame> {
    let first_byte = get_u8(buff)?;
    match first_byte {
        b'+' => {
            let line = read_line(buff)?;
            Frame::simple(line)
        },
        b'-' => {
            let line = read_line(buff)?;
            Frame::error(line)
        },
        b':' => {
            let line = read_line(buff)?;
            Frame::integer(line)
        },
        b'$' => todo!("Bulk Strings"),
        b'*' => todo!("Arrays"),
        _ => Err(Error::UnsupportedFrameType),
    }
}

fn get_u8(buff: &mut Cursor<&[u8]>) -> Result<u8> {
    if !buff.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(buff.get_u8())
}

fn read_line<'a>(buff: &mut Cursor<&'a [u8]>) -> Result<&'a [u8]> {
    let start = buff.position() as usize;
    let buff_ref = *buff.get_ref();

    let Some(cr_pos) = memchr(b'\r', &buff_ref[start..]) else {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\r\\n not found."
        )));
    };

    let expected_lf_pos = start + cr_pos + 1;

    if memchr(b'\n', &buff_ref[start..expected_lf_pos]).is_some() {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\n found in wrong position."
        )));
    }

    if buff_ref.len() <= expected_lf_pos {
        return Err(Error::Incomplete);
    }

    if buff_ref[expected_lf_pos] != b'\n' {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; \\n not found after \\r."
        )));
    }

    buff.set_position((expected_lf_pos + 1) as u64);

    Ok(&buff_ref[start..expected_lf_pos - 1])
}

#[cfg(test)]
mod tests {
    use crate::frame::{parse, read_line, Error, Frame};
    use claims::{assert_err, assert_ok};
    use proptest::prelude::{any, Strategy};
    use proptest::proptest;
    use std::io::Cursor;

    #[test]
    fn read_line_crlf_order_invalid() {
        // Arrange
        let buff = b"unimportant\n\r";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let line = read_line(&mut buff);

        // Assert
        assert_err!(&line);
        assert!(matches!(line, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn read_line_no_cr_at_all_invalid() {
        // Arrange
        let buff = b"unimportant";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let line = read_line(&mut buff);

        // Assert
        assert_err!(&line);
        assert!(matches!(line, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn read_line_missing_lf_invalid() {
        // Arrange
        let buff = b"unimportant\r";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let line = read_line(&mut buff);

        // Assert
        assert_err!(&line);
        assert!(matches!(line, Err(Error::Incomplete)));
    }

    #[test]
    fn read_line_content_contains_cr_invalid() {
        // Arrange
        let buff = b"unimportant\runimportant\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let line = read_line(&mut buff);

        // Assert
        assert_err!(&line);
        assert!(matches!(line, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn read_line_content_contains_lf_invalid() {
        // Arrange
        let buff = b"unimportant\nunimportant\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let line = read_line(&mut buff);

        // Assert
        assert_err!(&line);
        assert!(matches!(line, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn parse_empty_buf_invalid() {
        // Arrange
        let buff = b"";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::Incomplete)));
    }

    #[test]
    fn parse_multiple_frames() {
        // Arrange
        let buff = b"+simple\r\n-error\r\n:123\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let string_frame = parse(&mut buff);
        let error_frame = parse(&mut buff);
        let integer_frame = parse(&mut buff);
        // no more bytes to parse
        let should_be_error = parse(&mut buff);

        assert_ok!(&string_frame);
        if let Ok(Frame::Simple(content)) = string_frame {
            assert_eq!(content, "simple");
        } else {
            panic!("Expected Frame::Simple variant for first frame");
        }

        assert_ok!(&error_frame);
        if let Ok(Frame::Error(content)) = error_frame {
            assert_eq!(content, "error");
        } else {
            panic!("Expected Frame::Error variant for second frame");
        }

        assert_ok!(&integer_frame);
        if let Ok(Frame::Integer(num)) = integer_frame {
            assert_eq!(num, 123);
        } else {
            panic!("Expected Frame::Integer variant for third frame");
        }

        assert_err!(&should_be_error);
        assert!(matches!(should_be_error, Err(Error::Incomplete)));
    }

    #[test]
    fn parse_unsupported_frame_type_invalid() {
        // Arrange
        let buff = b"!content\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnsupportedFrameType)));
    }

    #[test]
    fn parse_simple_string_frame_valid() {
        // Arrange
        let buff = b"+Hello World\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Simple(content)) = frame {
            assert_eq!(content, "Hello World");
        } else {
            panic!("Expected Frame::Simple variant");
        }
    }

    #[test]
    fn parse_simple_string_empty_valid() {
        // Arrange
        let buff = b"+\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Simple(content)) = frame {
            assert_eq!(content, "");
        } else {
            panic!("Expected Frame::Simple variant");
        }
    }

    #[test]
    fn parse_simple_error_frame_valid() {
        // Arrange
        let buff = b"-ERR unknown command 'asdf'\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

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
        let buff = b":123\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, 123);
        } else {
            panic!("Expected Frame::Error variant");
        }
    }

    #[test]
    fn parse_integer_frame_with_leading_zeros() {
        // Arrange
        let buff = b":000123\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, 123);
        } else {
            panic!("Expected Frame::Integer variant");
        }
    }

    #[test]
    fn parse_integer_frame_zero_valid() {
        // Arrange
        let buff = b":0\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, 0);
        } else {
            panic!("Expected Frame::Error variant");
        }
    }

    #[test]
    fn parse_integer_frame_negative_zero_valid() {
        // Arrange
        let buff = b":-0\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Integer(content)) = frame {
            assert_eq!(content, 0);
        } else {
            panic!("Expected Frame::Error variant");
        }
    }

    #[test]
    fn parse_integer_frame_with_positive_signal_valid() {
        // Arrange
        let buff = b":+123\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

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
        let buff = b":-123\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

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
        let buff = b":9223372036854775807\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

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
        let buff = b":-9223372036854775808\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

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
        let buff = b":12a3\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn parse_integer_frame_large_invalid() {
        // Arrange
        // i64::MAX + 1
        let buff = b":9223372036854775808\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn parse_integer_frame_empty_invalid() {
        // Arrange
        let buff = b":\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnexpectedError(_))));
    }

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
                panic!("Expected Frame::Error variant")
            }
        }

        #[test]
        fn integer_frame_valid(frame_bytes in valid_integer_content_strategy()) {
            // Arrange
            let line = frame_bytes.as_slice();

            // Act
            let frame = Frame::integer(line);
            // Assert
            assert_ok!(&frame);
            if let Ok(Frame::Integer(content)) = frame {
                let expected_content = std::str::from_utf8(line)
                .unwrap()
                .parse::<i64>()
                .unwrap();
                assert_eq!(content, expected_content);
            } else {
                panic!("Expected Frame::Integer variant");
            }
        }
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

    fn valid_integer_content_strategy() -> impl Strategy<Value = Vec<u8>> {
        any::<i64>().prop_map(|num| num.to_string().into_bytes())
    }
}
