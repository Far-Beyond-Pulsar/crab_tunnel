use std::net::SocketAddr;
use std::time::Duration;

use crab_tunnel_client::{accept_and_punch, full_hole_punch, HolePunchClient};
use crab_tunnel_core::protocol::Message;
use crab_tunnel_core::RendezvousServer;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn start_server() -> (SocketAddr, RendezvousServer) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .try_init();

    let server = RendezvousServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind server");
    let addr = server.local_addr().unwrap();
    let s = server.clone();
    tokio::spawn(async move { s.run().await.expect("server run") });
    tokio::time::sleep(Duration::from_millis(200)).await;
    (addr, server)
}

async fn make_client(server_addr: SocketAddr) -> HolePunchClient {
    HolePunchClient::connect(server_addr, None)
        .await
        .expect("create client")
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_starts_and_accepts_registrations() {
    let (addr, _server) = start_server().await;

    let alice = make_client(addr).await;
    let public = alice.register("int-alice").await.unwrap();
    assert!(public.port() > 0, "server must assign a public address");

    let bob = make_client(addr).await;
    let public = bob.register("int-bob").await.unwrap();
    assert!(public.port() > 0);

    let peers = alice.list_peers().await.unwrap();
    assert!(peers.contains(&"int-alice".into()));
    assert!(peers.contains(&"int-bob".into()));
}

#[tokio::test]
async fn full_hole_punch_flow() {
    let (addr, _server) = start_server().await;

    let alice_handle = {
        let addr = addr;
        tokio::spawn(async move { accept_and_punch(addr, "int-alice").await })
    };

    let bob_handle = {
        let addr = addr;
        tokio::spawn(async move { full_hole_punch(addr, "int-bob", "int-alice").await })
    };

    let (alice_ret, bob_ret) = tokio::join!(alice_handle, bob_handle);

    let (alice_sock, connecting_peer_id, alice_peer_addr) =
        alice_ret.expect("alice join").unwrap();
    let (bob_sock, bob_peer_addr) = bob_ret.expect("bob join").unwrap();

    assert_eq!(connecting_peer_id, "int-bob");
    assert_eq!(alice_peer_addr.port(), bob_sock.local_addr().unwrap().port(),
        "alice's punched port must match bob's socket port");
    assert_eq!(bob_peer_addr.port(), alice_sock.local_addr().unwrap().port(),
        "bob's punched port must match alice's socket port");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let a_addr = alice_peer_addr;
    let b_addr = bob_peer_addr;

    // Exchange a few datagrams with retry
    let msg_a = b"hello from A";
    let mut buf = vec![0u8; 65535];

    // Alice sends to Bob
    for _ in 0..10 {
        if alice_sock.send_to(msg_a, a_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Bob receives from Alice
    let (len, from) = tokio::time::timeout(Duration::from_secs(3), bob_sock.recv_from(&mut buf))
        .await
        .expect("B: timeout reading from A")
        .unwrap();
    assert_eq!(from, b_addr, "B should receive from A's address");
    assert_eq!(&buf[..len], msg_a, "B should receive A's message");

    // Bob sends to Alice
    let msg_b = b"hello from B";
    bob_sock.send_to(msg_b, b_addr).await.unwrap();

    let (len, from) = tokio::time::timeout(Duration::from_secs(3), alice_sock.recv_from(&mut buf))
        .await
        .expect("A: timeout reading from B")
        .unwrap();
    assert_eq!(from, a_addr, "A should receive from B's address");
    assert_eq!(&buf[..len], msg_b, "A should receive B's message");
}

#[tokio::test]
async fn relay_communication() {
    let (addr, _server) = start_server().await;

    let alice = make_client(addr).await;
    alice.register("relay-a").await.unwrap();

    let bob = make_client(addr).await;
    bob.register("relay-b").await.unwrap();

    let payload = b"relayed data payload".to_vec();
    alice.relay_to("relay-b", payload.clone()).await.unwrap();

    let (msg, _from) = bob.recv_any().await.unwrap();
    match msg {
        Message::RelayData { data, .. } => {
            assert_eq!(data, payload, "relayed data must match");
        }
        other => panic!("expected RelayData, got {other:?}"),
    }
}

#[tokio::test]
async fn relay_bidirectional() {
    let (addr, _server) = start_server().await;

    let alice = make_client(addr).await;
    alice.register("relay-bi-a").await.unwrap();

    let bob = make_client(addr).await;
    bob.register("relay-bi-b").await.unwrap();

    alice.relay_to("relay-bi-b", b"from A".to_vec()).await.unwrap();
    let (msg, _) = bob.recv_any().await.unwrap();
    assert!(matches!(msg, Message::RelayData { ref data, .. } if data == b"from A"));

    bob.relay_to("relay-bi-a", b"from B".to_vec()).await.unwrap();
    let (msg, _) = alice.recv_any().await.unwrap();
    assert!(matches!(msg, Message::RelayData { ref data, .. } if data == b"from B"));
}

#[tokio::test]
async fn three_peers_concurrent() {
    let (addr, _server) = start_server().await;

    async fn register_peer(addr: SocketAddr, id: &str) -> HolePunchClient {
        let c = make_client(addr).await;
        c.register(id).await.unwrap();
        c
    }

    let a = register_peer(addr, "alpha").await;
    let _b = register_peer(addr, "beta").await;
    let g = register_peer(addr, "gamma").await;

    let peers = a.list_peers().await.unwrap();
    assert_eq!(peers.len(), 3, "all three peers visible");
    assert!(peers.contains(&"alpha".into()));
    assert!(peers.contains(&"beta".into()));
    assert!(peers.contains(&"gamma".into()));

    let beta_handle = {
        let addr = addr;
        tokio::spawn(async move {
            let c = make_client(addr).await;
            c.register("beta-2").await.unwrap();
            let peer_addr = c.connect_to_peer("alpha").await.unwrap();
            c.punch_to(peer_addr).await
        })
    };

    let incoming = a.accept_incoming().await.unwrap();
    assert_eq!(incoming.peer_id, "beta-2");
    a.punch_to(incoming.peer_addr).await.unwrap();
    beta_handle.await.unwrap().unwrap();

    let peers = g.list_peers().await.unwrap();
    assert!(!peers.is_empty(), "gamma still sees peers");
}

#[tokio::test]
async fn connect_to_nonexistent_peer() {
    let (addr, _server) = start_server().await;

    let client = make_client(addr).await;
    client.register("lonely").await.unwrap();

    let result = client.connect_to_peer("does-not-exist").await;
    assert!(
        result.is_err(),
        "connecting to a non-existent peer should fail"
    );
}

#[tokio::test]
async fn client_receives_incoming_while_connecting() {
    let (addr, _server) = start_server().await;

    let alice = make_client(addr).await;
    alice.register("alice-incoming").await.unwrap();

    // Charlie tries to connect to Alice while Alice is also connecting to Bob
    let charlie_handle = {
        let addr = addr;
        tokio::spawn(async move {
            let c = make_client(addr).await;
            c.register("charlie-incoming").await.unwrap();
            let peer_addr = c.connect_to_peer("alice-incoming").await.unwrap();
            c.punch_to(peer_addr).await
        })
    };

    // Alice should get the PunchRequest from Charlie
    let incoming = alice.accept_incoming().await.unwrap();
    assert_eq!(incoming.peer_id, "charlie-incoming");
    alice.punch_to(incoming.peer_addr).await.unwrap();

    charlie_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn incoming_gets_queued_during_register() {
    let (addr, _server) = start_server().await;

    let alice = make_client(addr).await;
    alice.register("alice-queue").await.unwrap();

    // Someone tries to connect to Alice while she's doing something else
    let charlie_handle = {
        let addr = addr;
        tokio::spawn(async move {
            let c = make_client(addr).await;
            c.register("charlie-queue").await.unwrap();
            let peer_addr = c.connect_to_peer("alice-queue").await.unwrap();
            c.punch_to(peer_addr).await
        })
    };

    // Give Charlie time to connect and the PunchRequest to arrive
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The PunchRequest should be in the socket buffer or in the queue
    let incoming = alice.accept_incoming().await.unwrap();
    assert_eq!(incoming.peer_id, "charlie-queue");
    alice.punch_to(incoming.peer_addr).await.unwrap();

    charlie_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn heartbeat_prevents_cleanup() {
    let (addr, _server) = start_server().await;

    let client = make_client(addr).await;
    client.register("hb-peer").await.unwrap();

    for _ in 0..5 {
        client.send_heartbeat().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let peers = client.list_peers().await.unwrap();
    assert!(peers.contains(&"hb-peer".into()));
}

#[tokio::test]
async fn socket_options_are_set() {
    use crab_tunnel_core::create_punch_socket;

    let socket = create_punch_socket("127.0.0.1:0".parse().unwrap()).unwrap();
    let std_socket = socket.into_std().unwrap();
    let err = std_socket.take_error().unwrap();
    assert!(err.is_none(), "socket should have no errors");
    drop(std_socket);
}

#[tokio::test]
async fn two_independent_pairs() {
    let (addr, _server) = start_server().await;

    // Pair 1
    let (a1_res, b1_res) = tokio::join!(
        accept_and_punch(addr, "pair-a"),
        full_hole_punch(addr, "pair-b", "pair-a"),
    );
    let (a1, _, a1_peer) = a1_res.unwrap();
    let (b1, _b1_peer) = b1_res.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut buf = vec![0u8; 65535];
    a1.send_to(b"msg-ab", a1_peer).await.unwrap();
    let (len, from) = tokio::time::timeout(Duration::from_secs(3), b1.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..len], b"msg-ab");
    assert_eq!(from.port(), a1.local_addr().unwrap().port());

    // Pair 2 (on the same server)
    let (a2_res, b2_res) = tokio::join!(
        accept_and_punch(addr, "pair-c"),
        full_hole_punch(addr, "pair-d", "pair-c"),
    );
    let (a2, _, a2_peer) = a2_res.unwrap();
    let (b2, _b2_peer) = b2_res.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    a2.send_to(b"msg-cd", a2_peer).await.unwrap();
    let (len, from) = tokio::time::timeout(Duration::from_secs(3), b2.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..len], b"msg-cd");
    assert_eq!(from.port(), a2.local_addr().unwrap().port());
}
