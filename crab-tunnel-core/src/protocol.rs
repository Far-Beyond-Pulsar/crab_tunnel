use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Message {
    Register {
        peer_id: String,
    },
    RegisterAck {
        public_addr: SocketAddr,
    },
    ConnectRequest {
        target_peer_id: String,
        requester_peer_id: String,
    },
    ConnectResponse {
        peer_addr: SocketAddr,
        peer_id: String,
    },
    PunchRequest {
        from_peer_id: String,
        from_addr: SocketAddr,
    },
    PunchAck,
    Heartbeat,
    ListPeers,
    PeerList {
        peers: Vec<String>,
    },
    Error {
        message: String,
    },
    RelayData {
        target_peer_id: String,
        data: Vec<u8>,
    },
}

pub fn encode(msg: &Message) -> Result<Vec<u8>, bincode::Error> {
    bincode::serialize(msg)
}

pub fn decode(bytes: &[u8]) -> Result<Message, bincode::Error> {
    bincode::deserialize(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_register() {
        let msg = Message::Register { peer_id: "alice".into() };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_roundtrip_register_ack() {
        let msg = Message::RegisterAck {
            public_addr: "192.168.1.100:54321".parse().unwrap(),
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_roundtrip_connect_request() {
        let msg = Message::ConnectRequest {
            target_peer_id: "bob".into(),
            requester_peer_id: "alice".into(),
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_roundtrip_connect_response() {
        let msg = Message::ConnectResponse {
            peer_addr: "10.0.0.1:9000".parse().unwrap(),
            peer_id: "bob".into(),
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_roundtrip_punch_request() {
        let msg = Message::PunchRequest {
            from_peer_id: "charlie".into(),
            from_addr: "10.0.0.2:1234".parse().unwrap(),
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_roundtrip_all_simple() {
        for msg in [
            Message::PunchAck,
            Message::Heartbeat,
            Message::ListPeers,
            Message::Error { message: "something went wrong".into() },
            Message::PeerList {
                peers: vec!["alice".into(), "bob".into()],
            },
            Message::RelayData {
                target_peer_id: "dave".into(),
                data: vec![1, 2, 3, 4],
            },
        ] {
            let bytes = encode(&msg).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(decoded, msg, "round-trip failed for {msg:?}");
        }
    }

    #[test]
    fn test_encode_invalid_data() {
        assert!(decode(b"").is_err());
        assert!(decode(b"garbage").is_err());
    }

    #[test]
    fn test_large_peer_id() {
        let peer_id = "a".repeat(4096);
        let msg = Message::Register { peer_id };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_large_relay_data() {
        let data = vec![0xABu8; 65536];
        let msg = Message::RelayData {
            target_peer_id: "big-data-peer".into(),
            data,
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_ipv6_addresses() {
        let msg = Message::RegisterAck {
            public_addr: "[::1]:8080".parse().unwrap(),
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);

        let msg = Message::PunchRequest {
            from_peer_id: "ipv6-peer".into(),
            from_addr: "[2001:db8::1]:9000".parse().unwrap(),
        };
        let bytes = encode(&msg).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(decoded, msg);
    }
}
