use super::*;

struct Endless {
    bytes: usize,
}
impl Read for Endless {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        buffer.fill(7);
        self.bytes += buffer.len();
        Ok(buffer.len())
    }
}

#[test]
fn fixed_publication_reader_bounds_even_an_unending_source_to_one_extra_byte() {
    for maximum in [44, 108, 116, 118, 140] {
        let mut source = Endless { bytes: 0 };
        let bytes = read_fixed(&mut source, maximum).unwrap();
        assert_eq!(source.bytes, maximum + 1);
        assert_eq!(bytes, vec![7; maximum + 1]);
        assert!(checked_file(&bytes, b"VSP1", maximum).is_err());
        for length in [0, maximum - 1, maximum, maximum + 1] {
            let input = vec![1; length];
            assert_eq!(read_fixed(input.as_slice(), maximum).unwrap(), input);
        }
    }
}

#[test]
fn fixed_publication_reader_propagates_io_failure_without_returning_partial_success() {
    struct Failing {
        first: bool,
    }
    impl Read for Failing {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.first {
                self.first = false;
                buffer[0] = 1;
                Ok(1)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected read failure",
                ))
            }
        }
    }
    assert_eq!(
        read_fixed(Failing { first: true }, 140).unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
}
