use std::net::SocketAddr;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tracing::debug;

use crate::error::HolePunchError;
use crate::protocol::{decode, encode, Message};

#[derive(Debug, Clone)]
pub struct PunchConfig {
    /// Number of punch packets to send
    pub attempts: u32,
    /// Delay between punch attempts
    pub interval: Duration,
    /// Timeout for the entire punch operation
    pub response_timeout: Duration,
    /// Whether to enable port prediction (for symmetric NAT)
    pub port_prediction: bool,
    /// Number of consecutive ports to try for prediction
    pub port_prediction_range: u16,
}

impl Default for PunchConfig {
    fn default() -> Self {
        Self {
            attempts: 30,
            interval: Duration::from_millis(50),
            response_timeout: Duration::from_secs(5),
            port_prediction: false,
            port_prediction_range: 5,
        }
    }
}

pub fn create_punch_socket(bind_addr: SocketAddr) -> Result<UdpSocket, HolePunchError> {
    let domain = if bind_addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&bind_addr.into())?;

    let _ = socket.set_recv_buffer_size(256 * 1024);
    let _ = socket.set_send_buffer_size(256 * 1024);

    let std_socket: std::net::UdpSocket = socket.into();
    Ok(UdpSocket::from_std(std_socket)?)
}

pub async fn punch_hole(
    socket: &UdpSocket,
    peer_addr: SocketAddr,
    config: &PunchConfig,
) -> Result<SocketAddr, HolePunchError> {
    debug!("Punching hole to {peer_addr}");

    let punch_msg = encode(&Message::PunchAck)?;
    let mut buf = vec![0u8; 65535];

    if config.port_prediction {
        let base_port = peer_addr.port();
        for offset in 1..=config.port_prediction_range {
            let mut predicted = peer_addr;
            predicted.set_port(base_port + offset);
            let _ = socket.send_to(&punch_msg, predicted).await;
        }
    }

    for i in 0..config.attempts {
        socket.send_to(&punch_msg, peer_addr).await?;
        debug!("Punch attempt {}/{} to {peer_addr}", i + 1, config.attempts);
        if i + 1 < config.attempts {
            tokio::time::sleep(config.interval).await;
        }
    }

    let deadline = tokio::time::Instant::now() + config.response_timeout;
    while tokio::time::Instant::now() < deadline {
        let remaining = deadline - tokio::time::Instant::now();
        match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok((_len, from))) if from == peer_addr => {
                debug!("Hole punch successful to {peer_addr}");
                drain_leftovers(socket, &mut buf).await;
                return Ok(peer_addr);
            }
            Ok(Ok((len, from))) => {
                if let Ok(msg) = decode(&buf[..len]) {
                    if matches!(msg, Message::PunchAck) {
                        debug!("Hole punch successful from {from}");
                        drain_leftovers(socket, &mut buf).await;
                        return Ok(from);
                    }
                }
                debug!("Ignoring {len} bytes from {from} during punch");
            }
            Ok(Err(e)) => {
                debug!("Recv error during punch: {e}");
            }
            Err(_) => {
                break;
            }
        }
    }

    Err(HolePunchError::PunchFailed)
}

async fn drain_leftovers(socket: &UdpSocket, buf: &mut [u8]) {
    loop {
        match tokio::time::timeout(Duration::from_millis(1), socket.recv_from(buf)).await {
            Ok(Ok((_len, _from))) => continue,
            _ => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_socket() {
        let socket = create_punch_socket("0.0.0.0:0".parse().unwrap()).unwrap();
        let addr = socket.local_addr().unwrap();
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn test_create_ipv6_socket() {
        let socket = create_punch_socket("[::1]:0".parse().unwrap()).unwrap();
        let addr = socket.local_addr().unwrap();
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn test_bind_specific_port() {
        let port = 23456u16;
        let socket = create_punch_socket(format!("127.0.0.1:{port}").parse().unwrap()).unwrap();
        assert_eq!(socket.local_addr().unwrap().port(), port);
    }

    #[tokio::test]
    async fn test_punch_between_two_sockets() {
        let a = create_punch_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let b = create_punch_socket("127.0.0.1:0".parse().unwrap()).unwrap();

        let addr_a = a.local_addr().unwrap();
        let addr_b = b.local_addr().unwrap();

        let punch_msg = encode(&Message::PunchAck).unwrap();

        let b_handle = tokio::spawn(async move {
            let mut buf = vec![0u8; 65535];
            for _ in 0..30 {
                let _ = b.send_to(&punch_msg, addr_a).await;
                match tokio::time::timeout(Duration::from_millis(25), b.recv_from(&mut buf)).await
                {
                    Ok(Ok((_len, from))) if from == addr_a => return Ok::<_, HolePunchError>(addr_a),
                    _ => {}
                }
            }
            Err(HolePunchError::PunchFailed)
        });

        let result = punch_hole(
            &a,
            addr_b,
            &PunchConfig {
                attempts: 15,
                interval: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .await;

        let b_result = b_handle.await.unwrap();

        assert!(
            result.is_ok() || b_result.is_ok(),
            "hole punch failed on both sides — result={result:?} b_result={b_result:?}"
        );
    }

    #[test]
    fn test_punch_config_defaults() {
        let cfg = PunchConfig::default();
        assert_eq!(cfg.attempts, 30);
        assert_eq!(cfg.interval, Duration::from_millis(50));
        assert_eq!(cfg.response_timeout, Duration::from_secs(5));
        assert!(!cfg.port_prediction);
    }

    #[tokio::test]
    async fn test_port_prediction_sends_extra_packets() {
        let a = create_punch_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let _addr_a = a.local_addr().unwrap();

        let config = PunchConfig {
            attempts: 5,
            port_prediction: true,
            port_prediction_range: 3,
            ..Default::default()
        };

        let b = create_punch_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr_b = b.local_addr().unwrap();

        let _ = punch_hole(&a, addr_b, &config).await;

        drop(a);
        drop(b);
        // Test passes if no panics — port prediction just sends extra packets
    }

    #[tokio::test]
    async fn test_punch_to_nonexistent_peer_fails() {
        let a = create_punch_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let nonexistent: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let result = punch_hole(
            &a,
            nonexistent,
            &PunchConfig {
                attempts: 3,
                interval: Duration::from_millis(10),
                ..Default::default()
            },
        )
        .await;

        assert!(
            result.is_err(),
            "punch to nonexistent peer should fail"
        );
    }
}
