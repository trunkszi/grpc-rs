//! A reset delivered while another handler reads must cancel queued dispatch.
use std::{
    cell::Cell,
    collections::VecDeque,
    io::{self, Read, Write},
    rc::Rc,
    time::Duration,
};

use grpc::{
    CompressionAlgorithm, Metadata, SessionConfig, SyncServer,
    core::{GrpcClientSession, transport::SyncTransport},
};

struct ScriptedTransport(VecDeque<Vec<u8>>);
impl Read for ScriptedTransport {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let Some(chunk) = self.0.front_mut() else {
            return Err(io::ErrorKind::WouldBlock.into());
        };
        let n = out.len().min(chunk.len());
        out[..n].copy_from_slice(&chunk[..n]);
        chunk.drain(..n);
        if chunk.is_empty() {
            self.0.pop_front();
        }
        Ok(n)
    }
}
impl Write for ScriptedTransport {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl SyncTransport for ScriptedTransport {
    fn set_nonblocking(&mut self, _: bool) -> io::Result<()> {
        Ok(())
    }
    fn set_read_timeout(&mut self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
    fn set_write_timeout(&mut self, _: Option<Duration>) -> io::Result<()> {
        Ok(())
    }
    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(None)
    }
    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        Ok(None)
    }
    fn peer_endpoint(&self) -> io::Result<String> {
        Ok("scripted peer".into())
    }
}
fn drain(client: &mut GrpcClientSession) -> anyhow::Result<Vec<u8>> {
    let mut wire = Vec::new();
    for _ in 0..100 {
        let chunk = client.mem_send()?;
        if chunk.is_empty() {
            return Ok(wire);
        }
        wire.extend_from_slice(chunk);
    }
    anyhow::bail!("client never quiesced")
}
#[test]
fn reset_queued_rpc_never_invokes_handler() -> anyhow::Result<()> {
    let mut client = GrpcClientSession::new(SessionConfig::default())?;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(client.start_request(
            "localhost",
            "/echo/Call",
            &Metadata::new(),
            None,
            CompressionAlgorithm::Identity,
            false,
        )?);
    }
    let headers = drain(&mut client)?;
    client.cancel_stream(ids[1], 8)?;
    client.send_message(ids[0], b"first", CompressionAlgorithm::Identity)?;
    client.half_close(ids[0])?;
    client.send_message(ids[2], b"healthy", CompressionAlgorithm::Identity)?;
    client.half_close(ids[2])?;
    let completion = drain(&mut client)?;
    let transport = ScriptedTransport(VecDeque::from([headers, completion]));
    let mut server = SyncServer::new(transport)?;
    let calls = Rc::new(Cell::new(0));
    let seen = Rc::clone(&calls);
    server.register_bidi_streaming(
        "/echo/Call",
        grpc::server::PayloadCodec,
        move |mut requests, _| {
            seen.set(seen.get() + 1);
            while requests
                .recv_next()
                .map_err(|e| grpc::Status::internal(e.to_string()))?
                .is_some()
            {}
            Ok(())
        },
    )?;
    assert!(server.poll_once()?);
    assert_eq!(calls.get(), 1);
    server.poll_once()?;
    assert_eq!(
        calls.get(),
        1,
        "cancelled queued request invoked application code"
    );
    assert!(server.poll_once()?);
    assert_eq!(calls.get(), 2, "healthy queued request must still run");
    Ok(())
}
