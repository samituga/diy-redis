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

    fn bulk(buff: &mut Cursor<&[u8]>) -> Result<Self> {
        let len_512_mb_no = 9;
        let len_crlf = 2;
        let limit = buff.position() + len_512_mb_no + len_crlf;
        let len = read_line_with_limit(buff, Some(limit as usize))?;
        let len = btoi::<i32>(len).map_err(|_| {
            Error::UnexpectedError(anyhow!("protocol error; invalid bulk string length digit"))
        })?;

        match len {
            -1 => Ok(Frame::Null),
            len if len < -1 => Err(Error::UnexpectedError(anyhow!(
                "protocol error; invalid bulk string length"
            ))),
            len => {
                let binary_line = read_binary_line(buff, len as usize)?.to_vec();

                if binary_line.len() != len as usize {
                    return Err(Error::UnexpectedError(anyhow!(
                        "protocol error; bulk string length mismatch"
                    )));
                }

                let bytes = Bytes::from(binary_line);
                Ok(Frame::Bulk(bytes))
            }
        }
    }
}

pub fn parse(buff: &mut Cursor<&[u8]>) -> Result<Frame> {
    let first_byte = get_u8(buff)?;
    match first_byte {
        b'+' => {
            let line = read_line(buff)?;
            Frame::simple(line)
        }
        b'-' => {
            let line = read_line(buff)?;
            Frame::error(line)
        }
        b':' => {
            let line = read_line(buff)?;
            Frame::integer(line)
        }
        b'$' => Frame::bulk(buff),
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
    read_line_with_limit(buff, None)
}

fn read_line_with_limit<'a>(buff: &mut Cursor<&'a [u8]>, limit: Option<usize>) -> Result<&'a [u8]> {
    let start = buff.position() as usize;
    let buff_ref = *buff.get_ref();
    let end = limit.unwrap_or(buff_ref.len());
    let end = end.min(buff_ref.len());

    let Some(cr_pos) = memchr(b'\r', &buff_ref[start..end]) else {
        return if limit.is_some() && limit.unwrap() > buff_ref.len() {
            Err(Error::UnexpectedError(anyhow!(
                "protocol error; \\r\\n not found."
            )))
        } else {
            Err(Error::Incomplete)
        };
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

fn read_binary_line<'a>(buff: &mut Cursor<&'a [u8]>, content_len: usize) -> Result<&'a [u8]> {
    if buff.remaining() < content_len + 2 {
        return Err(Error::Incomplete);
    }

    let start = buff.position() as usize;
    let end = start + content_len;
    let buff_ref = *buff.get_ref();
    let data = &buff_ref[start..end];

    buff.set_position(end as u64);

    let cr = buff.get_u8();
    let lf = buff.get_u8();
    if cr != b'\r' || lf != b'\n' {
        return Err(Error::UnexpectedError(anyhow!(
            "protocol error; missing final CRLF for bulk string"
        )));
    }

    Ok(data)
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
    fn read_line_incomplete_no_crlf_at_all_incomplete() {
        // Arrange
        let buff = b"unimportant";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let line = read_line(&mut buff);

        // Assert
        assert_err!(&line);
        assert!(matches!(line, Err(Error::Incomplete)));
    }

    #[test]
    fn read_line_missing_lf_incomplete() {
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
    fn parse_empty_buf_incomplete() {
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
        let buff = b"+simple\r\n-error\r\n:123\r\n$11\r\nbulk string\r\n+simple\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let string_frame = parse(&mut buff);
        let error_frame = parse(&mut buff);
        let integer_frame = parse(&mut buff);
        let bulk_string_frame = parse(&mut buff);
        let string_repeat_frame = parse(&mut buff);
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

        assert_ok!(&bulk_string_frame);
        if let Ok(Frame::Bulk(content)) = bulk_string_frame {
            assert_eq!(content, "bulk string");
        } else {
            panic!("Expected Frame::Bulk variant for third frame");
        }

        assert_ok!(&string_repeat_frame);
        if let Ok(Frame::Simple(content)) = string_repeat_frame {
            assert_eq!(content, "simple");
        } else {
            panic!("Expected Frame::Simple variant for first frame");
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

    #[test]
    fn parse_bulk_string_starts_with_crlf_valid() {
        // Arrange
        let buff = b"$7\r\nhel\r\nlo\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Bulk(content)) = frame {
            assert_eq!(content, "hel\r\nlo");
        } else {
            panic!("Expected Frame::Bulk variant");
        }
    }

    #[test]
    fn parse_bulk_string_zero_length_valid() {
        // Arrange
        let buff = b"$0\r\n\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        if let Ok(Frame::Bulk(content)) = frame {
            assert_eq!(content, "");
        } else {
            panic!("Expected Frame::Bulk variant");
        }
    }

    #[test]
    fn parse_bulk_string_null_valid() {
        // Arrange
        let buff = b"$-1\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_ok!(&frame);
        assert!(matches!(frame, Ok(Frame::Null)));
    }

    #[test]
    fn parse_large_bulk_string_valid() {
        // Arrange
        let len: usize = 10 * 1024 * 1024; // 10MB
        let payload = vec![b'a'; len];
        let mut frame = Vec::new();
        frame.extend_from_slice(format!("${}\r\n", len).as_bytes());
        frame.extend_from_slice(&payload);
        frame.extend_from_slice(b"\r\n");

        let mut buff = Cursor::new(frame.as_slice());

        // Act
        let result = parse(&mut buff);

        // Assert
        assert_ok!(&result);
        if let Ok(Frame::Bulk(content)) = result {
            assert_eq!(content.len(), len);
            assert!(content.iter().all(|&b| b == b'a'));
        } else {
            panic!("Expected Frame::Bulk variant");
        }
        assert_eq!(buff.position(), frame.len() as u64);
    }

    #[test]
    fn parse_bulk_string_starts_with_crlf_invalid() {
        // Arrange
        let buff = b"$\r\n5\r\nhello\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn parse_bulk_string_missing_crlf_after_length_invalid() {
        // Arrange
        let buff = b"$5hello\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn parse_bulk_string_missing_final_crlf_incomplete() {
        // Arrange
        let buff = b"$5\r\nhello";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::Incomplete)));
    }

    // Still not sure if this test make sense, and what error to return in this scenario
    #[test]
    fn parse_bulk_string_length_mismatch_too_short_incomplete() {
        // Arrange
        let buff = b"$6\r\nhello\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::Incomplete)));
    }

    #[test]
    fn parse_bulk_string_length_mismatch_too_long_invalid() {
        // Arrange
        let buff = b"$5\r\nhello!\r\n";
        let mut buff = Cursor::new(buff.as_slice());

        // Act
        let frame = parse(&mut buff);

        // Assert
        assert_err!(&frame);
        assert!(matches!(frame, Err(Error::UnexpectedError(_))));
    }

    #[test]
    fn parse_bulk_string_length_less_than_negative_one_invalid() {
        // Arrange
        let buff = b"$-2\r\n";
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

        #[test]
        fn bulk_string_frame_valid((frame_bytes, expected_content) in valid_bulk_string_frame_strategy()) {
            // Arrange
            let line = frame_bytes.as_slice();
            let mut buff = Cursor::new(line);

            // Act
            let frame = Frame::bulk(&mut buff);
            // Assert
            assert_ok!(&frame);
            if let Ok(Frame::Bulk(content)) = frame {
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

    fn valid_bulk_string_frame_strategy() -> impl Strategy<Value = (Vec<u8>, String)> {
        proptest::collection::vec(any::<char>(), 0..341)
            .prop_map(|chars| chars.into_iter().collect::<String>())
            .prop_map(|content| {
                let frame = format!("{}\r\n{}\r\n", content.len(), content);
                (frame.into_bytes(), content)
            })
    }
}
