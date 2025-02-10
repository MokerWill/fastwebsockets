use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use fastwebsockets::FragmentCollector;
use fastwebsockets::Frame;
use fastwebsockets::handshake::client;
use fastwebsockets::OpCode;
use http_body_util::Empty;
use hyper::header::CONNECTION;
use hyper::header::UPGRADE;
use hyper::upgrade::Upgraded;
use hyper::Request;
use hyper_util::rt::TokioIo;
use rustls_pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
  fn execute(&self, fut: Fut) {
    tokio::task::spawn(fut);
  }
}

const UPSTREAM_DOMAIN: &str = "stream.binance.com";

async fn connect() -> Result<FragmentCollector<TokioIo<Upgraded>>> {
  let mut addr = String::from(UPSTREAM_DOMAIN);
  addr.push_str(":9443"); // Port number for binance stream

  let new_domain = ServerName::try_from(UPSTREAM_DOMAIN)?;
  let mut root_store = RootCertStore::empty();
  root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

  let config = ClientConfig::builder()
      .with_root_certificates(root_store)
      .with_no_client_auth();
  let tls_connector = TlsConnector::from(Arc::new(config));
  let tcp_stream = TcpStream::connect(&addr).await?;
  let tls_stream = tls_connector.connect(new_domain, tcp_stream).await?;

  let req = Request::builder()
      .method("GET")
      .uri(format!("wss://{}/ws/btcusdt@bookTicker", &addr)) //stream we want to subscribe to
      .header("Host", &addr)
      .header(UPGRADE, "websocket")
      .header(CONNECTION, "upgrade")
      .header(
        "Sec-WebSocket-Key",
        fastwebsockets::handshake::generate_key(),
      )
      .header("Sec-WebSocket-Version", "13")
      .body(Empty::<Bytes>::new())?;

  let (ws, _) = client(&SpawnExecutor, req, tls_stream).await?;
  Ok(FragmentCollector::new(ws))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
  let mut ws = connect().await?;

  loop {
    let msg = match ws.read_frame().await {
      Ok(msg) => msg,
      Err(e) => {
        println!("Error: {}", e);
        ws.write_frame(Frame::close_raw(vec![].into())).await?;
        break;
      }
    };

    match msg.opcode {
      OpCode::Text => {
        let payload =
            String::from_utf8(msg.payload.to_vec()).expect("Invalid UTF-8 data");
        // Normally deserialise from json here, print just to show it works
        println!("{:?}", payload);
      }
      OpCode::Close => {
        break;
      }
      _ => {}
    }
  }
  Ok(())
}
