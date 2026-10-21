//! An absolute socket deadline bounds TLS and frame I/O, including slow drips.
use std::cell::RefCell;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::rc::Rc;
use std::time::{Duration, Instant};

pub(super) struct DeadlineStream {
    stream: TcpStream,
    budget: Rc<RefCell<Budget>>,
}
struct Budget {
    deadline: Instant,
    application_cpu_since: Option<Instant>,
}
/// Pauses accounting for application CPU, while transport calls still consume
/// the same cumulative deadline. Ownership is independent of the TLS stream.
pub(super) struct ApplicationGuard {
    budget: Rc<RefCell<Budget>>,
}
impl Drop for ApplicationGuard {
    fn drop(&mut self) {
        let mut budget = self.budget.borrow_mut();
        if let Some(since) = budget.application_cpu_since.take() {
            budget.deadline += since.elapsed();
        }
    }
}
impl DeadlineStream {
    pub(super) fn new(stream: TcpStream, timeout: Duration) -> Self {
        Self {
            stream,
            budget: Rc::new(RefCell::new(Budget {
                deadline: Instant::now() + timeout,
                application_cpu_since: None,
            })),
        }
    }
    pub(super) fn application_guard(&self) -> io::Result<ApplicationGuard> {
        self.remaining()?;
        let mut budget = self.budget.borrow_mut();
        if budget.application_cpu_since.is_some() {
            return Err(io::Error::other("nested application I/O guard"));
        }
        budget.application_cpu_since = Some(Instant::now());
        Ok(ApplicationGuard {
            budget: self.budget.clone(),
        })
    }
    fn remaining(&self) -> io::Result<Duration> {
        let mut budget = self.budget.borrow_mut();
        if let Some(since) = budget.application_cpu_since {
            budget.deadline += since.elapsed();
            budget.application_cpu_since = Some(Instant::now());
        }
        budget
            .deadline
            .checked_duration_since(Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::TimedOut, "data connection deadline expired")
            })
    }
    fn io_finished(&self) {
        let mut budget = self.budget.borrow_mut();
        if budget.application_cpu_since.is_some() {
            budget.application_cpu_since = Some(Instant::now());
        }
    }
}
impl Read for DeadlineStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream.set_read_timeout(Some(self.remaining()?))?;
        let result = self.stream.read(bytes);
        self.io_finished();
        result
    }
}
impl Write for DeadlineStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        let result = self.stream.write(bytes);
        self.io_finished();
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.set_write_timeout(Some(self.remaining()?))?;
        let result = self.stream.flush();
        self.io_finished();
        result
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
        {
            let _guard = stream.application_guard().unwrap();
            std::thread::sleep(Duration::from_millis(250));
            stream.write_all(b"early").unwrap();
        }
        let mut early = [0; 5];
        client.read_exact(&mut early).unwrap();
        assert_eq!(&early, b"early");
        stream.write_all(b"result").unwrap();
        let mut result = [0; 6];
        client.read_exact(&mut result).unwrap();
        assert_eq!(&result, b"result");
        stream.budget.borrow_mut().deadline = Instant::now() - Duration::from_millis(1);
        assert!(stream.application_guard().is_err());
        assert!(stream.flush().is_err());
    }
    #[test]
    fn streaming_application_work_cannot_renew_blocked_socket_budget() {
        let (socket, _nonreading_client) = socket_pair();
        let mut stream = DeadlineStream::new(socket, Duration::from_millis(100));
        let _guard = stream.application_guard().unwrap();
        std::thread::sleep(Duration::from_millis(120));
        let chunk = [0_u8; 64 * 1024];
        let mut error = None;
        for _ in 0..1024 {
            if let Err(failure) = stream.write_all(&chunk) {
                error = Some(failure);
                break;
            }
        }
        assert!(matches!(
            error
                .expect("nonreading socket must exhaust its cumulative I/O budget")
                .kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ));
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
