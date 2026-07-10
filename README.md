<p align="center">
  <img src="assets/crab_tunnel.png" width="200" alt="crab-tunnel">
</p>

A UDP hole-punching library built with Tokio and socket2.

```
crab-tunnel-core          protocol types, server, hole-punch primitives
crab-tunnel-server        rendezvous server binary
crab-tunnel-client        high-level hole-punch client library
```

## Overview

UDP hole punching is a technique for establishing direct peer-to-peer UDP
communication between two hosts behind NAT routers. A well-known rendezvous
server helps each peer discover the other's public address. Both peers then
simultaneously send packets to each other, "punching a hole" through their
respective NATs so direct communication can proceed.

## Quick start

### Start the rendezvous server

```
cargo run -p crab-tunnel-server -- --bind 0.0.0.0:3478
```

### Full example: two peers + server

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

use crab_tunnel_client::{accept_and_punch, full_hole_punch, HolePunchClient};
use crab_tunnel_core::RendezvousServer;

#[tokio::main]
async fn main() {
    // 1. Start a rendezvous server on an ephemeral port
    let server = RendezvousServer::bind("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind server");
    let server_addr = server.local_addr().unwrap();
    tokio::spawn(async move { server.run().await.unwrap() });
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Server running on {server_addr}");

    // 2. Peer A waits for an incoming connection
    let a_handle = tokio::spawn(async move {
        let (socket, peer_id, peer_addr) =
            accept_and_punch(server_addr, "peer-a").await.unwrap();
        println!("A: connected to {peer_id} at {peer_addr}");
        socket
    });

    // 3. Peer B actively connects to A
    let b_handle = tokio::spawn(async move {
        let (socket, peer_addr) =
            full_hole_punch(server_addr, "peer-b", "peer-a").await.unwrap();
        println!("B: punched to {peer_addr}");
        socket
    });

    let (a_sock, b_sock) = tokio::join!(a_handle, b_handle).unwrap();
    let (a_addr, b_addr) = (
        a_sock.local_addr().unwrap(),
        b_sock.local_addr().unwrap(),
    );

    // 4. Communicate directly
    a_sock.send_to(b"ping", b_addr).await.unwrap();
    let mut buf = vec![0u8; 1024];
    let (n, from) = b_sock.recv_from(&mut buf).await.unwrap();
    println!("B received {:?} from {from}", &buf[..n]);
    assert_eq!(&buf[..n], b"ping");

    b_sock.send_to(b"pong", a_addr).await.unwrap();
    let (n, from) = a_sock.recv_from(&mut buf).await.unwrap();
    println!("A received {:?} from {from}", &buf[..n]);
    assert_eq!(&buf[..n], b"pong");

    println!("Direct peer-to-peer communication established!");
}
```

## API

### HolePunchClient

| Method | Description |
|---|---|
| `connect(server, bind_addr?)` | Create a client, optionally binding to a specific local address |
| `register(peer_id)` | Register with the rendezvous server; returns public address |
| `connect_to_peer(target_id)` | Request a connection to a registered peer; returns their address |
| `accept_incoming()` | Wait for another peer to request a connection to us |
| `punch_to(addr)` | Execute the hole punch against the given address |
| `send_heartbeat()` | Send a keep-alive heartbeat to the server |
| `list_peers()` | Get all registered peer IDs from the server |
| `relay_to(target_id, data)` | Send data through the server (TURN-like fallback) |
| `socket()` | Get a reference to the underlying `Arc<UdpSocket>` |

### Convenience functions

- `full_hole_punch(server, my_id, target_id)` — register, connect, and punch
- `accept_and_punch(server, my_id)` — register, wait for incoming, and punch

## How it works

```
Peer A                  Rendezvous Server                Peer B
  |                          |                              |
  |--- Register("alice") --> |                              |
  |<-- RegisterAck(addr) --- |                              |
  |                          |                              |
  |                          |   <--- Register("bob") ------|
  |                          |   ---- RegisterAck(addr) --> |
  |                          |                              |
  |                          |   <-- ConnectRequest("alice")|
  |<-- PunchRequest(bob) --- |                              |
  |                          |   ---- ConnectResponse ----> |
  |                          |                              |
  |=========== punch packets ==============================>|
  |<========================== punch packets ===============|
  |                          |                              |
  |==================== direct UDP ========================>|
  |<=================== direct UDP ========================|
```

Both peers exchange punch packets simultaneously. Each outgoing packet
creates a temporary NAT mapping that allows the peer's return traffic
to reach the local host. Once a packet gets through on either side the
"hole" is open and the peers can communicate directly.
