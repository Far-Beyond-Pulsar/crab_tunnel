use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crab_tunnel_core::error::HolePunchError;
use crab_tunnel_core::hole_punch::{create_punch_socket, punch_hole, PunchConfig};
use crab_tunnel_core::protocol::{decode, encode, Message};

#[derive(Debug, Clone)]
pub struct IncomingConnection {
    pub peer_id: String,
    pub peer_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct PeerConnection {
    pub peer_id: String,
    pub peer_addr: SocketAddr,
}

#[derive(Clone)]
pub struct HolePunchClient {
    socket: Arc<UdpSocket>,
    server_addr: SocketAddr,
    my_peer_id: Arc<Mutex<String>>,
    my_public_addr: Arc<Mutex<SocketAddr>>,
    pending_incoming: Arc<Mutex<VecDeque<IncomingConnection>>>,
    punch_config: PunchConfig,
    recv_timeout: Duration,
}

impl HolePunchClient {
    /// Create a new client wrapping an already-bound socket.
    pub fn new(socket: UdpSocket, server_addr: SocketAddr) -> Self {
        Self {
            socket: Arc::new(socket),
            server_addr,
            my_peer_id: Arc::new(Mutex::new(String::new())),
            my_public_addr: Arc::new(Mutex::new("0.0.0.0:0".parse().unwrap())),
            pending_incoming: Arc::new(Mutex::new(VecDeque::new())),
            punch_config: PunchConfig::default(),
            recv_timeout: Duration::from_secs(30),
        }
    }

    /// Convenience constructor: binds to an ephemeral port and returns a client.
    pub async fn connect(
        server_addr: SocketAddr,
        bind_addr: Option<SocketAddr>,
    ) -> Result<Self, HolePunchError> {
        let bind = bind_addr.unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
        let socket = create_punch_socket(bind)?;
        Ok(Self::new(socket, server_addr))
    }

    // -- Configuration -------------------------------------------------------

    pub fn set_punch_config(&mut self, config: PunchConfig) {
        self.punch_config = config;
    }

    pub fn punch_config(&self) -> &PunchConfig {
        &self.punch_config
    }

    pub fn set_recv_timeout(&mut self, timeout: Duration) {
        self.recv_timeout = timeout;
    }

    // -- Accessors -----------------------------------------------------------

    pub fn local_addr(&self) -> Result<SocketAddr, HolePunchError> {
        Ok(self.socket.local_addr()?)
    }

    pub async fn public_addr(&self) -> SocketAddr {
        *self.my_public_addr.lock().await
    }

    pub async fn peer_id(&self) -> String {
        self.my_peer_id.lock().await.clone()
    }

    pub fn socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    // -- Registration --------------------------------------------------------

    /// Register with the rendezvous server. Returns the public address observed
    /// by the server.
    pub async fn register(&self, peer_id: &str) -> Result<SocketAddr, HolePunchError> {
        *self.my_peer_id.lock().await = peer_id.to_string();

        self.send_msg(&Message::Register {
            peer_id: peer_id.to_string(),
        })
        .await?;

        loop {
            match self.recv_msg_from_server().await? {
                Message::RegisterAck { public_addr } => {
                    *self.my_public_addr.lock().await = public_addr;
                    info!("Registered as \"{peer_id}\" — public address: {public_addr}");
                    return Ok(public_addr);
                }
                Message::PunchRequest {
                    from_peer_id,
                    from_addr,
                } => {
                    debug!("Queuing incoming punch from {from_peer_id}");
                    self.pending_incoming
                        .lock()
                        .await
                        .push_back(IncomingConnection {
                            peer_id: from_peer_id,
                            peer_addr: from_addr,
                        });
                }
                Message::Error { message } => {
                    return Err(HolePunchError::Server(message));
                }
                _ => {}
            }
        }
    }

    // -- Connection ----------------------------------------------------------

    /// Request a connection to `target_peer_id`. Returns the target's socket
    /// address. The server also sends a `PunchRequest` to the target so it can
    /// begin punching back.
    pub async fn connect_to_peer(
        &self,
        target_peer_id: &str,
    ) -> Result<SocketAddr, HolePunchError> {
        let requester = self.peer_id().await;

        self.send_msg(&Message::ConnectRequest {
            target_peer_id: target_peer_id.to_string(),
            requester_peer_id: requester,
        })
        .await?;

        loop {
            match self.recv_msg_from_server().await? {
                Message::ConnectResponse { peer_addr, .. } => {
                    info!("Got peer address for \"{target_peer_id}\": {peer_addr}");
                    return Ok(peer_addr);
                }
                Message::Error { message } => {
                    return Err(HolePunchError::Server(message));
                }
                Message::PunchRequest {
                    from_peer_id,
                    from_addr,
                } => {
                    debug!("Queuing incoming punch from {from_peer_id} while connecting");
                    self.pending_incoming
                        .lock()
                        .await
                        .push_back(IncomingConnection {
                            peer_id: from_peer_id,
                            peer_addr: from_addr,
                        });
                }
                _ => {}
            }
        }
    }

    /// Block until another peer requests a connection to us.
    pub async fn accept_incoming(&self) -> Result<IncomingConnection, HolePunchError> {
        if let Some(conn) = self.pending_incoming.lock().await.pop_front() {
            debug!("Returning queued incoming from {}", conn.peer_id);
            return Ok(conn);
        }

        loop {
            match self.recv_msg_from_server().await? {
                Message::PunchRequest {
                    from_peer_id,
                    from_addr,
                } => {
                    info!("Incoming connection from \"{from_peer_id}\" at {from_addr}");
                    return Ok(IncomingConnection {
                        peer_id: from_peer_id,
                        peer_addr: from_addr,
                    });
                }
                _ => {}
            }
        }
    }

    /// Check for queued incoming connections without blocking.
    pub async fn has_pending_incoming(&self) -> bool {
        !self.pending_incoming.lock().await.is_empty()
    }

    // -- Hole punching -------------------------------------------------------

    /// Execute the hole punch against `peer_addr` using the stored config.
    pub async fn punch_to(&self, peer_addr: SocketAddr) -> Result<SocketAddr, HolePunchError> {
        info!("Punching hole to {peer_addr}");
        punch_hole(&self.socket, peer_addr, &self.punch_config).await
    }

    // -- Heartbeat / keep-alive ----------------------------------------------

    pub async fn send_heartbeat(&self) -> Result<(), HolePunchError> {
        self.send_msg(&Message::Heartbeat).await
    }

    // -- Relay ---------------------------------------------------------------

    /// Send data through the server to `target_peer_id` (TURN-like fallback).
    pub async fn relay_to(
        &self,
        target_peer_id: &str,
        data: Vec<u8>,
    ) -> Result<(), HolePunchError> {
        self.send_msg(&Message::RelayData {
            target_peer_id: target_peer_id.to_string(),
            data,
        })
        .await
    }

    // -- Peer discovery ------------------------------------------------------

    pub async fn list_peers(&self) -> Result<Vec<String>, HolePunchError> {
        self.send_msg(&Message::ListPeers).await?;
        loop {
            match self.recv_msg_from_server().await? {
                Message::PeerList { peers } => return Ok(peers),
                Message::PunchRequest {
                    from_peer_id,
                    from_addr,
                } => {
                    self.pending_incoming
                        .lock()
                        .await
                        .push_back(IncomingConnection {
                            peer_id: from_peer_id,
                            peer_addr: from_addr,
                        });
                }
                Message::Error { message } => return Err(HolePunchError::Server(message)),
                _ => {}
            }
        }
    }

    // -- Low-level I/O -------------------------------------------------------

    async fn send_msg(&self, msg: &Message) -> Result<(), HolePunchError> {
        let bytes = encode(msg)?;
        self.socket.send_to(&bytes, self.server_addr).await?;
        Ok(())
    }

    async fn recv_msg_from_server(&self) -> Result<Message, HolePunchError> {
        let mut buf = vec![0u8; 65535];

        loop {
            let result = tokio::time::timeout(self.recv_timeout, self.socket.recv_from(&mut buf))
                .await
                .map_err(|_| HolePunchError::Timeout)?
                .map_err(HolePunchError::Io)?;

            let (len, from) = result;

            if from != self.server_addr {
                debug!("Ignoring {len} bytes from non-server {from}");
                continue;
            }

            match decode(&buf[..len]) {
                Ok(msg) => return Ok(msg),
                Err(e) => {
                    warn!("Failed to decode server message: {e}");
                    continue;
                }
            }
        }
    }

    /// Receive any message (from any source). Useful in tests.
    pub async fn recv_any(&self) -> Result<(Message, SocketAddr), HolePunchError> {
        let mut buf = vec![0u8; 65535];
        let (len, from) = self.socket.recv_from(&mut buf).await?;
        let msg = decode(&buf[..len])?;
        Ok((msg, from))
    }
}

/// High-level helper: register, request a peer connection, and punch in one
/// call.  Returns the connected socket and the peer's address.
pub async fn full_hole_punch(
    server_addr: SocketAddr,
    my_peer_id: &str,
    target_peer_id: &str,
) -> Result<(Arc<UdpSocket>, SocketAddr), HolePunchError> {
    let client = HolePunchClient::connect(server_addr, None).await?;
    client.register(my_peer_id).await?;
    let peer_addr = client.connect_to_peer(target_peer_id).await?;
    let punched = client.punch_to(peer_addr).await?;
    Ok((client.socket(), punched))
}

/// High-level helper: register and wait for an incoming connection, then
/// punch back.  Returns the socket, the connecting peer's ID, and address.
pub async fn accept_and_punch(
    server_addr: SocketAddr,
    my_peer_id: &str,
) -> Result<(Arc<UdpSocket>, String, SocketAddr), HolePunchError> {
    let client = HolePunchClient::connect(server_addr, None).await?;
    client.register(my_peer_id).await?;
    let incoming = client.accept_incoming().await?;
    let punched = client.punch_to(incoming.peer_addr).await?;
    Ok((client.socket(), incoming.peer_id, punched))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crab_tunnel_core::RendezvousServer;

    async fn server_pair() -> (SocketAddr, RendezvousServer) {
        let server = RendezvousServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let s = server.clone();
        tokio::spawn(async move { s.run().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(100)).await;
        (addr, server)
    }

    async fn make_client(server_addr: SocketAddr) -> HolePunchClient {
        HolePunchClient::connect(server_addr, None)
            .await
            .expect("client connect")
    }

    #[tokio::test]
    async fn test_client_register() {
        let (addr, _server) = server_pair().await;
        let client = make_client(addr).await;
        let public = client.register("test-register").await.unwrap();
        assert!(public.port() > 0);
        assert_eq!(client.peer_id().await, "test-register");
        assert_eq!(client.public_addr().await, public);
    }

    #[tokio::test]
    async fn test_client_register_twice() {
        let (addr, _server) = server_pair().await;
        let client = make_client(addr).await;
        client.register("first").await.unwrap();
        client.register("second").await.unwrap();
        assert_eq!(client.peer_id().await, "second");
    }

    #[tokio::test]
    async fn test_client_connect_unknown() {
        let (addr, _server) = server_pair().await;
        let client = make_client(addr).await;
        client.register("client-a").await.unwrap();
        let result = client.connect_to_peer("nonexistent").await;
        assert!(result.is_err(), "connect to unknown peer should fail");
    }

    #[tokio::test]
    async fn test_client_list_peers() {
        let (addr, _server) = server_pair().await;
        let alice = make_client(addr).await;
        alice.register("alice").await.unwrap();

        let bob = make_client(addr).await;
        bob.register("bob").await.unwrap();

        let peers = alice.list_peers().await.unwrap();
        assert!(peers.contains(&"alice".into()), "alice sees alice");
        assert!(peers.contains(&"bob".into()), "alice sees bob");
    }

    #[tokio::test]
    async fn test_client_heartbeat() {
        let (addr, _server) = server_pair().await;
        let client = make_client(addr).await;
        client.register("hb-test").await.unwrap();
        client.send_heartbeat().await.unwrap();
        // no crash = success
    }

    #[tokio::test]
    async fn test_relay_between_two_clients() {
        let (addr, _server) = server_pair().await;
        let alice = make_client(addr).await;
        alice.register("relay-alice").await.unwrap();

        let bob = make_client(addr).await;
        bob.register("relay-bob").await.unwrap();

        // Alice relays data to Bob
        let payload = b"hello via relay".to_vec();
        tokio::spawn(async move {
            alice.relay_to("relay-bob", payload).await.unwrap();
        });

        // Bob receives the relay
        let (msg, _) = bob.recv_any().await.unwrap();
        match msg {
            Message::RelayData { data, .. } => {
                assert_eq!(data, b"hello via relay");
            }
            other => panic!("expected RelayData, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_connect_flow_between_two_clients() {
        let (addr, _server) = server_pair().await;

        let alice = make_client(addr).await;
        alice.register("alice").await.unwrap();

        let bob = make_client(addr).await;
        bob.register("bob").await.unwrap();

        let bob_handle = {
            let addr = addr;
            tokio::spawn(async move {
                let client = HolePunchClient::connect(addr, None).await.unwrap();
                client.register("bob-helper").await.unwrap();
                let peer_addr = client.connect_to_peer("alice").await.unwrap();
                client.punch_to(peer_addr).await
            })
        };

        let incoming = alice.accept_incoming().await.unwrap();
        assert_eq!(
            incoming.peer_id, "bob-helper",
            "should receive punch request from bob-helper"
        );

        let _ = alice.punch_to(incoming.peer_addr).await;

        let bob_result = bob_handle.await.unwrap();
        assert!(
            bob_result.is_ok(),
            "bob's punch should succeed: {bob_result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // High-level helper tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_full_hole_punch_convenience() {
        let (addr, _server) = server_pair().await;

        let alice_handle = {
            let addr = addr;
            tokio::spawn(async move {
                accept_and_punch(addr, "alice-conv").await
            })
        };

        let bob_handle = {
            let addr = addr;
            tokio::spawn(async move {
                full_hole_punch(addr, "bob-conv", "alice-conv").await
            })
        };

        let (alice_result, bob_result) =
            tokio::join!(alice_handle, bob_handle);

        let (_alice_sock, alice_peer_id, _alice_peer_addr) = alice_result.expect("alice join").unwrap();
        let (_bob_sock, _bob_peer_addr) = bob_result.expect("bob join").unwrap();

        assert_eq!(alice_peer_id, "bob-conv", "alice should connect with bob");
    }
}
