//! Only descriptor-bound, idempotent recovery exchanges may resolve one lost
//! reply automatically. Voting traffic retains its original single deadline.
use super::*;

pub(super) fn exchange<T>(kind: u8, mut attempt: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    let result = attempt();
    let resumable = matches!(
        kind,
        HISTORY_BEGIN
            | HISTORY_RECORD
            | HISTORY_FINISH
            | INSTALL_BEGIN
            | INSTALL_RECORD
            | INSTALL_BATCH
            | INSTALL_FINISH
            | VIEW_CHANGE_BEGIN
            | VIEW_CHANGE_RECORD
            | VIEW_CHANGE_BATCH
            | MEMBERSHIP_BEGIN
            | MEMBERSHIP_SNAPSHOT
            | MEMBERSHIP_BATCH
    );
    match result {
        Err(error)
            if resumable
                && matches!(
                    error.kind(),
                    io::ErrorKind::TimedOut
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::BrokenPipe
                ) =>
        {
            attempt()
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_recovery_requests_retry_a_lost_reply_and_never_more_than_once() {
        for kind in [
            HISTORY_BEGIN,
            HISTORY_RECORD,
            HISTORY_FINISH,
            INSTALL_BEGIN,
            INSTALL_RECORD,
            INSTALL_BATCH,
            INSTALL_FINISH,
            VIEW_CHANGE_BEGIN,
            VIEW_CHANGE_RECORD,
            VIEW_CHANGE_BATCH,
            MEMBERSHIP_BEGIN,
            MEMBERSHIP_SNAPSHOT,
            MEMBERSHIP_BATCH,
        ] {
            let mut calls = 0;
            let value = exchange(kind, || {
                calls += 1;
                if calls == 1 {
                    Err(io::Error::from(io::ErrorKind::UnexpectedEof))
                } else {
                    Ok(17)
                }
            })
            .unwrap();
            assert_eq!(value, 17);
            assert_eq!(calls, 2);
            let mut calls = 0;
            assert!(
                exchange::<()>(kind, || {
                    calls += 1;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                })
                .is_err()
            );
            assert_eq!(calls, 2);
        }
        for kind in [
            PREPARE,
            COMMIT,
            PRIMARY_PROBE,
            TRANSFER,
            TAIL,
            HEARTBEAT,
            REPLICA_STATUS,
            255,
        ] {
            let mut calls = 0;
            assert!(
                exchange::<()>(kind, || {
                    calls += 1;
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                })
                .is_err()
            );
            assert_eq!(calls, 1);
        }
    }
    #[test]
    fn rejected_request_or_bad_authenticated_response_is_not_retried() {
        for error in [
            io::ErrorKind::InvalidData,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::ConnectionRefused,
            io::ErrorKind::InvalidInput,
        ] {
            let mut calls = 0;
            assert!(
                exchange::<()>(INSTALL_FINISH, || {
                    calls += 1;
                    Err(io::Error::from(error))
                })
                .is_err()
            );
            assert_eq!(calls, 1);
        }
    }
}
