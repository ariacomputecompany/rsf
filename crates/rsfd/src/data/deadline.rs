//! An absolute socket deadline bounds TLS and frame I/O, including slow drips.
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

pub(super) struct DeadlineStream {
    stream: TcpStream,
    deadline: Instant,
}
impl DeadlineStream {
    pub(super) fn new(stream: TcpStream, timeout: Duration) -> Self {
        Self {
            stream,
            deadline: Instant::now() + timeout,
        }
    }
    /// Preserve one cumulative TLS/frame/response I/O budget while synchronous
    /// application work uses no socket time. This is not an execution timeout.
    pub(super) fn execute<T>(&mut self, action: impl FnOnce() -> T) -> io::Result<T> {
        self.remaining()?;
        let started = Instant::now();
        let outcome = action();
        self.deadline += started.elapsed();
        Ok(outcome)
    }
    fn remaining(&self) -> io::Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::TimedOut, "data connection deadline expired")
            })
    }
}

impl Read for DeadlineStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream.set_read_timeout(Some(self.remaining()?))?;
        self.stream.read(bytes)
    }
}
impl Write for DeadlineStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        self.stream.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        self.stream.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        (listener.accept().unwrap().0, client)
    }
    #[test]
    fn application_work_preserves_remaining_io_budget_and_an_expired_frame_cannot_execute() {
        let (socket, mut client) = socket_pair();
        let mut stream = DeadlineStream::new(socket, Duration::from_millis(200));
        assert_eq!(
            stream
                .execute(|| {
                    std::thread::sleep(Duration::from_millis(250));
                    42
                })
                .unwrap(),
            42
        );
        stream.write_all(b"result").unwrap();
        let mut result = [0; 6];
        client.read_exact(&mut result).unwrap();
        assert_eq!(&result, b"result");
        stream.deadline = Instant::now() - Duration::from_millis(1);
        let mut executed = false;
        assert!(stream.execute(|| executed = true).is_err());
        assert!(!executed);
        assert!(stream.flush().is_err());
    }
    #[test]
    fn slow_drips_do_not_renew_the_cumulative_io_budget() {
        let (socket, mut client) = socket_pair();
        let mut stream = DeadlineStream::new(socket, Duration::from_millis(100));
        let sender = std::thread::spawn(move || {
            for _ in 0..8 {
                if client.write_all(b"x").is_err() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(30));
            }
        });
        let mut received = 0;
        let error = loop {
            let mut byte = [0];
            match stream.read_exact(&mut byte) {
                Ok(()) => received += 1,
                Err(error) => break error,
            }
        };
        sender.join().unwrap();
        assert!(matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ));
        assert!(received < 8, "slow drips must not renew the socket budget");
    }
}
