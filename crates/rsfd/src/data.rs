use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use rsf_wire::{WireAdapter, WireCodec, WireDecision, WireOperationKind};

const MAX_DATA_FRAME_BYTES: u32 = 64 * 1024;

pub struct DataPlaneListener {
    listener: TcpListener,
    codec: WireCodec,
    adapter: WireAdapter,
}

impl DataPlaneListener {
    pub fn bind(addr: SocketAddr) -> Result<Self, String> {
        let listener = TcpListener::bind(addr)
            .map_err(|err| format!("failed to bind daemon data listener {addr}: {err}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|err| format!("failed to configure daemon data listener {addr}: {err}"))?;
        Ok(Self {
            listener,
            codec: WireCodec,
            adapter: WireAdapter,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound data listener exposes local address")
    }

    pub fn serve_pending(&self, out: &mut impl Write) -> Result<usize, String> {
        let mut served = 0_usize;
        loop {
            match self.listener.accept() {
                Ok((stream, peer)) => {
                    let response = serve_data_stream(stream, &self.codec, &self.adapter)?;
                    emit_data_plane_response(out, peer, &response)?;
                    served = served.saturating_add(1);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => return Ok(served),
                Err(err) => return Err(format!("failed to accept daemon data request: {err}")),
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataPlaneResponse {
    Accepted {
        protocol: &'static str,
        operation_kind: &'static str,
        body_bytes: u64,
    },
    Rejected {
        protocol: &'static str,
        reason: String,
    },
    Corrupt {
        reason: String,
    },
}

fn serve_data_stream(
    mut stream: TcpStream,
    codec: &WireCodec,
    adapter: &WireAdapter,
) -> Result<DataPlaneResponse, String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("failed to configure data read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("failed to configure data write timeout: {err}"))?;

    let response = read_data_frame(&mut stream)
        .and_then(|frame| {
            codec
                .decode_and_admit(&frame, adapter)
                .map_err(|err| err.message)
        })
        .map(data_plane_response)
        .unwrap_or_else(|reason| DataPlaneResponse::Corrupt { reason });
    let body = render_data_plane_response(&response);
    stream
        .write_all(body.as_bytes())
        .map_err(|err| format!("failed to write daemon data response: {err}"))?;
    stream
        .flush()
        .map_err(|err| format!("failed to flush daemon data response: {err}"))?;
    Ok(response)
}

fn read_data_frame(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut len = [0_u8; 4];
    stream
        .read_exact(&mut len)
        .map_err(|err| format!("failed to read daemon data frame length: {err}"))?;
    let len = u32::from_le_bytes(len);
    if len == 0 {
        return Err("daemon data frame length must be greater than zero".to_string());
    }
    if len > MAX_DATA_FRAME_BYTES {
        return Err(format!(
            "daemon data frame length {} exceeds {} bytes",
            len, MAX_DATA_FRAME_BYTES
        ));
    }
    let mut frame = vec![0_u8; len as usize];
    stream
        .read_exact(&mut frame)
        .map_err(|err| format!("failed to read daemon data frame body: {err}"))?;
    Ok(frame)
}

fn data_plane_response(decision: WireDecision) -> DataPlaneResponse {
    match decision {
        WireDecision::Accepted(envelope) => DataPlaneResponse::Accepted {
            protocol: envelope.protocol.as_str(),
            operation_kind: operation_kind_label(envelope.operation_kind),
            body_bytes: envelope.body_bytes,
        },
        WireDecision::Rejected { protocol, reason } => DataPlaneResponse::Rejected {
            protocol: protocol.as_str(),
            reason,
        },
    }
}

fn operation_kind_label(kind: WireOperationKind) -> &'static str {
    match kind {
        WireOperationKind::Read => "read",
        WireOperationKind::Write => "write",
        WireOperationKind::Admin => "admin",
    }
}

fn render_data_plane_response(response: &DataPlaneResponse) -> String {
    match response {
        DataPlaneResponse::Accepted {
            protocol,
            operation_kind,
            body_bytes,
        } => format!(
            "RSF/1 accepted protocol={} operation_kind={} body_bytes={}\n",
            protocol, operation_kind, body_bytes
        ),
        DataPlaneResponse::Rejected { protocol, reason } => {
            format!("RSF/1 rejected protocol={} reason={}\n", protocol, reason)
        }
        DataPlaneResponse::Corrupt { reason } => format!("RSF/1 corrupt reason={reason}\n"),
    }
}

fn emit_data_plane_response(
    out: &mut impl Write,
    peer: SocketAddr,
    response: &DataPlaneResponse,
) -> Result<(), String> {
    match response {
        DataPlaneResponse::Accepted {
            protocol,
            operation_kind,
            body_bytes,
        } => writeln!(
            out,
            "daemon data peer={} status=accepted protocol={} operation_kind={} body_bytes={}",
            peer, protocol, operation_kind, body_bytes
        ),
        DataPlaneResponse::Rejected { protocol, reason } => writeln!(
            out,
            "daemon data peer={} status=rejected protocol={} reason={}",
            peer, protocol, reason
        ),
        DataPlaneResponse::Corrupt { reason } => {
            writeln!(
                out,
                "daemon data peer={} status=corrupt reason={}",
                peer, reason
            )
        }
    }
    .map_err(|err| err.to_string())?;
    out.flush().map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsf_wire::{RawFrame, WireProtocol};

    #[test]
    fn accepts_canonical_wire_frames() {
        let listener = DataPlaneListener::bind("127.0.0.1:0".parse().expect("data listener addr"))
            .expect("bind data listener");
        let addr = listener.local_addr();
        let frame = WireCodec
            .encode(&RawFrame::new(
                WireProtocol::Sql,
                "app-client",
                Some("app".to_string()),
                "SELECT * FROM accounts",
                None,
                128,
            ))
            .expect("encode wire frame");

        let client = std::thread::spawn(move || send_data_frame(addr, frame));
        let mut out = Vec::new();
        wait_for_data_request(&listener, &mut out, "accepted");
        let response = client.join().expect("client thread");
        let output = String::from_utf8(out).expect("stdout is utf8");

        assert!(response.contains("RSF/1 accepted"));
        assert!(response.contains("protocol=sql-wire"));
        assert!(response.contains("operation_kind=read"));
        assert!(output.contains("daemon data"));
        assert!(output.contains("status=accepted"));
        assert!(output.contains("protocol=sql-wire"));
    }

    #[test]
    fn rejects_invalid_wire_requests() {
        let listener = DataPlaneListener::bind("127.0.0.1:0".parse().expect("data listener addr"))
            .expect("bind data listener");
        let addr = listener.local_addr();
        let frame = WireCodec
            .encode(&RawFrame::new(
                WireProtocol::Blob,
                "app-client",
                Some("app".to_string()),
                "PUT sha256:content",
                None,
                512,
            ))
            .expect("encode wire frame");

        let client = std::thread::spawn(move || send_data_frame(addr, frame));
        let mut out = Vec::new();
        wait_for_data_request(&listener, &mut out, "rejected");
        let response = client.join().expect("client thread");
        let output = String::from_utf8(out).expect("stdout is utf8");

        assert!(response.contains("RSF/1 rejected"));
        assert!(response.contains("protocol=blob-stream"));
        assert!(response.contains("idempotency key is required"));
        assert!(output.contains("status=rejected"));
        assert!(output.contains("protocol=blob-stream"));
    }

    fn send_data_frame(addr: SocketAddr, frame: Vec<u8>) -> String {
        let mut stream = TcpStream::connect(addr).expect("connect data listener");
        let len = u32::try_from(frame.len())
            .expect("test frame length fits u32")
            .to_le_bytes();
        stream.write_all(&len).expect("write frame length");
        stream.write_all(&frame).expect("write frame");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    fn wait_for_data_request(listener: &DataPlaneListener, out: &mut Vec<u8>, label: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let served = listener
                .serve_pending(out)
                .unwrap_or_else(|err| panic!("serve {label} data request: {err}"));
            if served > 0 {
                assert_eq!(served, 1);
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {label} data request"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
