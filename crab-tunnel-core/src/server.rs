use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::HolePunchError;
use crate::protocol::{decode, encode, Message};

const PEER_TIMEOUT: Duration = Duration::from_secs(120);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
struct PeerInfo {
    addr: SocketAddr,
    last_seen: Instant,
}

#[derive(Debug, Clone)]
pub struct RendezvousServer {
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
    socket: Arc<UdpSocket>,
}

impl RendezvousServer {
    pub async fn bind(addr: SocketAddr) -> Result<Self, HolePunchError> {
        use socket2::{Domain, Protocol, Socket, Type};

        let socket2 = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        socket2.set_reuse_address(true)?;
        socket2.set_nonblocking(true)?;
        socket2.bind(&addr.into())?;

        let std_socket: std::net::UdpSocket = socket2.into();
        let socket = UdpSocket::from_std(std_socket)?;

        info!("Rendezvous server bound to {}", socket.local_addr()?);

        Ok(Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            socket: Arc::new(socket),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, HolePunchError> {
        Ok(self.socket.local_addr()?)
    }

    pub async fn run(self) -> Result<(), HolePunchError> {
        let peers = self.peers.clone();
        let cleanup_handle = tokio::spawn(async move {
            Self::cleanup_loop(peers).await;
        });

        let mut buf = vec![0u8; 65535];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, addr)) => {
                    let data = &buf[..len];
                    match decode(data) {
                        Ok(msg) => {
                            if let Err(e) = self.handle_message(&self.socket, addr, msg).await {
                                warn!("Error handling message from {addr}: {e}");
                            }
                        }
                        Err(e) => {
                            debug!("Failed to decode message from {addr}: {e}");
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving datagram: {e}");
                    break;
                }
            }
        }

        cleanup_handle.await.ok();
        Ok(())
    }

    async fn handle_message(
        &self,
        socket: &UdpSocket,
        from_addr: SocketAddr,
        msg: Message,
    ) -> Result<(), HolePunchError> {
        debug!("Received from {from_addr}: {msg:?}");

        match msg {
            Message::Register { peer_id } => {
                self.handle_register(socket, from_addr, &peer_id).await?;
            }
            Message::ConnectRequest {
                target_peer_id,
                requester_peer_id,
            } => {
                self.handle_connect_request(socket, from_addr, &target_peer_id, &requester_peer_id)
                    .await?;
            }
            Message::Heartbeat => {
                self.handle_heartbeat(from_addr).await;
            }
            Message::ListPeers => {
                self.handle_list_peers(socket, from_addr).await?;
            }
            Message::RelayData {
                target_peer_id,
                data,
            } => {
                self.handle_relay(socket, &target_peer_id, data).await?;
            }
            Message::PunchAck => {
                debug!("PunchAck from {from_addr}");
            }
            _ => {
                warn!("Unexpected message from {from_addr}: {msg:?}");
                Self::send_msg(
                    socket,
                    from_addr,
                    &Message::Error {
                        message: "unexpected message type".into(),
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_register(
        &self,
        socket: &UdpSocket,
        from_addr: SocketAddr,
        peer_id: &str,
    ) -> Result<(), HolePunchError> {
        {
            let mut peers = self.peers.write().await;
            peers.insert(
                peer_id.to_string(),
                PeerInfo {
                    addr: from_addr,
                    last_seen: Instant::now(),
                },
            );
        }
        info!("Peer registered: \"{peer_id}\" at {from_addr}");

        Self::send_msg(
            socket,
            from_addr,
            &Message::RegisterAck {
                public_addr: from_addr,
            },
        )
        .await?;

        Ok(())
    }

    async fn handle_connect_request(
        &self,
        socket: &UdpSocket,
        from_addr: SocketAddr,
        target_peer_id: &str,
        requester_peer_id: &str,
    ) -> Result<(), HolePunchError> {
        let peers = self.peers.read().await;

        if let Some(target) = peers.get(target_peer_id) {
            info!(
                "Forwarding connection \"{requester_peer_id}\" -> \"{target_peer_id}\" \
                 ({from_addr} -> {})",
                target.addr
            );

            Self::send_msg(
                socket,
                target.addr,
                &Message::PunchRequest {
                    from_peer_id: requester_peer_id.to_string(),
                    from_addr,
                },
            )
            .await?;

            Self::send_msg(
                socket,
                from_addr,
                &Message::ConnectResponse {
                    peer_addr: target.addr,
                    peer_id: target_peer_id.to_string(),
                },
            )
            .await?;
        } else {
            warn!("Connect request for unknown peer: \"{target_peer_id}\"");
            Self::send_msg(
                socket,
                from_addr,
                &Message::Error {
                    message: format!("peer '{target_peer_id}' not found"),
                },
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_heartbeat(&self, from_addr: SocketAddr) {
        let mut peers = self.peers.write().await;
        for info in peers.values_mut() {
            if info.addr == from_addr {
                info.last_seen = Instant::now();
                debug!("Heartbeat from {from_addr}");
                return;
            }
        }
    }

    async fn handle_list_peers(
        &self,
        socket: &UdpSocket,
        from_addr: SocketAddr,
    ) -> Result<(), HolePunchError> {
        let peers = self.peers.read().await;
        let peer_list: Vec<String> = peers.keys().cloned().collect();
        debug!("Listing {count} peers for {from_addr}", count = peer_list.len());
        Self::send_msg(
            socket,
            from_addr,
            &Message::PeerList { peers: peer_list },
        )
        .await?;
        Ok(())
    }

    async fn handle_relay(
        &self,
        socket: &UdpSocket,
        target_peer_id: &str,
        data: Vec<u8>,
    ) -> Result<(), HolePunchError> {
        let peers = self.peers.read().await;

        if let Some(target) = peers.get(target_peer_id) {
            debug!("Relaying {} bytes to \"{target_peer_id}\"", data.len());
            Self::send_msg(socket, target.addr, &Message::RelayData {
                target_peer_id: target_peer_id.to_string(),
                data,
            })
            .await?;
        } else {
            warn!("Relay target \"{target_peer_id}\" not found");
        }

        Ok(())
    }

    async fn send_msg(
        socket: &UdpSocket,
        to: SocketAddr,
        msg: &Message,
    ) -> Result<(), HolePunchError> {
        let bytes = encode(msg)?;
        socket.send_to(&bytes, to).await?;
        Ok(())
    }

    async fn cleanup_loop(peers: Arc<RwLock<HashMap<String, PeerInfo>>>) {
        loop {
            tokio::time::sleep(CLEANUP_INTERVAL).await;
            let mut peers = peers.write().await;
            let now = Instant::now();
            let expired: Vec<String> = peers
                .iter()
                .filter(|(_, info)| now.duration_since(info.last_seen) > PEER_TIMEOUT)
                .map(|(id, _)| id.clone())
                .collect();

            for id in &expired {
                info!("Removing stale peer: \"{id}\"");
                peers.remove(id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn server_and_addr() -> (RendezvousServer, SocketAddr) {
        let server = RendezvousServer::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = server.local_addr().unwrap();
        let s = server.clone();
        tokio::spawn(async move { s.run().await.unwrap() });
        tokio::time::sleep(Duration::from_millis(100)).await;
        (server, addr)
    }

    async fn send_and_recv(
        server_addr: SocketAddr,
        msg: &Message,
    ) -> (Message, SocketAddr) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bytes = encode(msg).unwrap();
        socket.send_to(&bytes, server_addr).await.unwrap();

        let mut buf = vec![0u8; 65535];
        let (len, from) = tokio::time::timeout(Duration::from_secs(3), socket.recv_from(&mut buf))
            .await
            .expect("timeout waiting for response")
            .unwrap();
        let response = decode(&buf[..len]).unwrap();
        (response, from)
    }

    async fn register_and_recv(addr: SocketAddr, peer_id: &str) -> Message {
        let (resp, _) = send_and_recv(addr, &Message::Register {
            peer_id: peer_id.into(),
        })
        .await;
        resp
    }

    #[tokio::test]
    async fn test_register_peer() {
        let (_server, addr) = server_and_addr().await;
        let resp = register_and_recv(addr, "alice").await;
        assert!(
            matches!(resp, Message::RegisterAck { .. }),
            "expected RegisterAck, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn test_register_returns_public_addr() {
        let (_server, addr) = server_and_addr().await;
        match register_and_recv(addr, "alice").await {
            Message::RegisterAck { public_addr } => {
                assert!(public_addr.port() > 0, "expected valid port");
            }
            other => panic!("expected RegisterAck, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_connect_known_peer() {
        let (_server, addr) = server_and_addr().await;
        register_and_recv(addr, "alice").await;
        register_and_recv(addr, "bob").await;

        let (resp, _) = send_and_recv(
            addr,
            &Message::ConnectRequest {
                target_peer_id: "alice".into(),
                requester_peer_id: "bob".into(),
            },
        )
        .await;
        assert!(
            matches!(resp, Message::ConnectResponse { .. }),
            "expected ConnectResponse, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn test_connect_unknown_peer() {
        let (_server, addr) = server_and_addr().await;
        register_and_recv(addr, "alice").await;

        let (resp, _) = send_and_recv(
            addr,
            &Message::ConnectRequest {
                target_peer_id: "nonexistent".into(),
                requester_peer_id: "alice".into(),
            },
        )
        .await;
        assert!(
            matches!(resp, Message::Error { .. }),
            "expected Error, got {resp:?}"
        );
    }

    #[tokio::test]
    async fn test_list_peers() {
        let (_server, addr) = server_and_addr().await;
        register_and_recv(addr, "alice").await;
        register_and_recv(addr, "bob").await;

        let (resp, _) = send_and_recv(addr, &Message::ListPeers).await;
        match resp {
            Message::PeerList { peers } => {
                assert!(peers.contains(&"alice".to_string()));
                assert!(peers.contains(&"bob".to_string()));
            }
            other => panic!("expected PeerList, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_duplicate_register_updates() {
        let (_server, addr) = server_and_addr().await;

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bytes = encode(&Message::Register {
            peer_id: "alice".into(),
        })
        .unwrap();
        socket.send_to(&bytes, addr).await.unwrap();
        let mut buf = vec![0u8; 65535];
        socket.recv_from(&mut buf).await.unwrap();

        let bytes2 = encode(&Message::Register {
            peer_id: "alice".into(),
        })
        .unwrap();
        socket.send_to(&bytes2, addr).await.unwrap();
        socket.recv_from(&mut buf).await.unwrap();

        let (resp, _) = send_and_recv(addr, &Message::ListPeers).await;
        match resp {
            Message::PeerList { peers } => {
                assert_eq!(peers.len(), 1, "should still be one peer");
                assert_eq!(peers[0], "alice");
            }
            other => panic!("expected PeerList, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_heartbeat_does_not_crash() {
        let (_server, addr) = server_and_addr().await;
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bytes = encode(&Message::Heartbeat).unwrap();
        socket.send_to(&bytes, addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn test_punch_request_sent_to_target() {
        let (_server, addr) = server_and_addr().await;

        let alice_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let alice_bytes = encode(&Message::Register {
            peer_id: "alice".into(),
        })
        .unwrap();
        alice_socket.send_to(&alice_bytes, addr).await.unwrap();
        let mut buf = vec![0u8; 65535];
        alice_socket.recv_from(&mut buf).await.unwrap();

        let bob_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bob_local = bob_socket.local_addr().unwrap();
        let bob_bytes = encode(&Message::ConnectRequest {
            target_peer_id: "alice".into(),
            requester_peer_id: "bob".into(),
        })
        .unwrap();
        bob_socket.send_to(&bob_bytes, addr).await.unwrap();
        // consume the ConnectResponse so it doesn't interfere
        bob_socket.recv_from(&mut buf).await.unwrap();

        let resp = tokio::time::timeout(
            Duration::from_secs(3),
            async {
                loop {
                    let (len, from) = alice_socket.recv_from(&mut buf).await.unwrap();
                    if from == addr {
                        return decode(&buf[..len]).unwrap();
                    }
                }
            },
        )
        .await
        .expect("alice did not receive PunchRequest");

        match resp {
            Message::PunchRequest {
                from_peer_id,
                from_addr,
            } => {
                assert_eq!(from_peer_id, "bob");
                assert_eq!(from_addr, bob_local, "PunchRequest from_addr should match Bob's socket");
            }
            other => panic!("expected PunchRequest, got {other:?}"),
        }
    }
}
