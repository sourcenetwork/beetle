//! Implements handling of the [bitswap protocol]((https://github.com/ipfs/specs/blob/master/BITSWAP.md)). Based on go-ipfs.
//!
//! Supports the versions `1.0.0`, `1.1.0` and `1.2.0`.

use std::collections::HashSet;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::iroh_metrics::{bitswap::BitswapMetrics, core::MRecorder};
use ahash::AHashMap;
use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use handler::{BitswapHandler, HandlerEvent};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{
    CloseConnection, ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm,
    NetworkBehaviour, NotifyHandler, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use self::client::Config as ClientConfig;
use self::message::BitswapMessage;
use self::network::Network;
use self::network::OutEvent;
use self::protocol::ProtocolConfig;
use self::server::{Config as ServerConfig, Server};

mod block;
mod client;
mod error;
mod handler;
mod iroh_metrics;
mod network;
mod prefix;
mod protocol;
mod server;

pub mod message;
pub mod peer_task_queue;

pub use self::block::{tests::*, Block};
pub use self::client::Client;
pub use self::protocol::ProtocolId;

const DIAL_BACK_OFF: Duration = Duration::from_secs(10 * 60);

type DialMap = AHashMap<
    PeerId,
    Vec<(
        usize,
        oneshot::Sender<std::result::Result<Option<ProtocolId>, String>>,
    )>,
>;

#[derive(Debug, Clone)]
pub struct Bitswap<S: Store> {
    network: Network,
    protocol_config: ProtocolConfig,
    idle_timeout: Duration,
    peers: Arc<Mutex<AHashMap<PeerId, PeerState>>>,
    /// Every live connection per peer, tracked from the swarm's own
    /// established/closed events. `peers` records one connection and one
    /// protocol; this records reachability, which is what deciding whether to
    /// dial and whether a peer is still usable actually depends on.
    connections: Arc<Mutex<AHashMap<PeerId, HashSet<ConnectionId>>>>,
    dials: Arc<Mutex<DialMap>>,
    /// Set to true when dialing should be disabled because we have reached the conn limit.
    pause_dialing: bool,
    client: Client<S>,
    server: Option<Server<S>>,
    incoming_messages: mpsc::Sender<(PeerId, BitswapMessage)>,
    peers_connected: mpsc::Sender<PeerId>,
    peers_disconnected: mpsc::Sender<PeerId>,
    _workers: Arc<Vec<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum PeerState {
    Connected(ConnectionId),
    Responsive(ConnectionId, ProtocolId),
    #[default]
    Disconnected,
    DialFailure(Instant),
}

impl PeerState {
    fn is_connected(self) -> bool {
        matches!(self, PeerState::Connected(_) | PeerState::Responsive(_, _))
    }
}

#[derive(Debug)]
pub struct Config {
    pub client: ClientConfig,
    /// If no server config is set, the server is disabled.
    pub server: Option<ServerConfig>,
    pub protocol: ProtocolConfig,
    pub idle_timeout: Duration,
}

impl Config {
    pub fn default_client_mode() -> Self {
        Config {
            server: None,
            ..Default::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            client: ClientConfig::default(),
            server: Some(ServerConfig::default()),
            protocol: ProtocolConfig::default(),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

#[async_trait]
pub trait Store: Debug + Clone + Send + Sync + 'static {
    async fn get_size(&self, cid: &Cid) -> Result<usize>;
    async fn get(&self, cid: &Cid) -> Result<Block>;
    async fn has(&self, cid: &Cid) -> Result<bool>;
}

impl<S: Store> Bitswap<S> {
    pub async fn new(self_id: PeerId, store: S, config: Config) -> Self {
        let network = Network::new(self_id);
        let (server, cb) = if let Some(config) = config.server {
            let server = Server::new(network.clone(), store.clone(), config).await;
            let cb = server.received_blocks_cb();
            (Some(server), Some(cb))
        } else {
            (None, None)
        };
        let client = Client::new(network.clone(), store, cb, config.client).await;

        let (sender_msg, mut receiver_msg) = mpsc::channel(2048);
        let (sender_con, mut receiver_con) = mpsc::channel(2048);
        let (sender_dis, mut receiver_dis) = mpsc::channel(2048);

        let mut workers = Vec::new();
        workers.push(tokio::task::spawn({
            let server = server.clone();
            let client = client.clone();

            async move {
                // process messages serially but without blocking the p2p loop
                while let Some((peer, message)) = receiver_msg.recv().await {
                    if let Some(ref server) = server {
                        futures::future::join(
                            client.receive_message(&peer, &message),
                            server.receive_message(&peer, &message),
                        )
                        .await;
                    } else {
                        client.receive_message(&peer, &message).await;
                    }
                }
            }
        }));

        workers.push(tokio::task::spawn({
            let server = server.clone();
            let client = client.clone();

            async move {
                // process messages serially but without blocking the p2p loop
                while let Some(peer) = receiver_con.recv().await {
                    if let Some(ref server) = server {
                        futures::future::join(
                            client.peer_connected(&peer),
                            server.peer_connected(&peer),
                        )
                        .await;
                    } else {
                        client.peer_connected(&peer).await;
                    }
                }
            }
        }));

        workers.push(tokio::task::spawn({
            let server = server.clone();
            let client = client.clone();

            async move {
                // process messages serially but without blocking the p2p loop
                while let Some(peer) = receiver_dis.recv().await {
                    if let Some(ref server) = server {
                        futures::future::join(
                            client.peer_disconnected(&peer),
                            server.peer_disconnected(&peer),
                        )
                        .await;
                    } else {
                        client.peer_disconnected(&peer).await;
                    }
                }
            }
        }));

        Bitswap {
            network,
            protocol_config: config.protocol,
            idle_timeout: config.idle_timeout,
            peers: Default::default(),
            connections: Default::default(),
            dials: Default::default(),
            pause_dialing: false,
            server,
            client,
            incoming_messages: sender_msg,
            peers_connected: sender_con,
            peers_disconnected: sender_dis,
            _workers: Arc::new(workers),
        }
    }

    pub fn server(&self) -> Option<&Server<S>> {
        self.server.as_ref()
    }

    pub fn client(&self) -> &Client<S> {
        &self.client
    }

    pub async fn stop(self) -> Result<()> {
        self.network.stop();
        if let Some(server) = self.server {
            futures::future::try_join(self.client.stop(), server.stop()).await?;
        } else {
            self.client.stop().await?;
        }

        Ok(())
    }

    pub async fn notify_new_blocks(&self, blocks: &[Block]) -> Result<()> {
        self.client.notify_new_blocks(blocks).await?;
        if let Some(ref server) = self.server {
            server.notify_new_blocks(blocks).await?;
        }

        Ok(())
    }

    /// Called on identify events from swarm, informing us about available protocols of this peer.
    pub fn on_identify(&self, peer: &PeerId, protocols: &[String]) {
        if let Some(PeerState::Connected(conn_id)) = self.get_peer_state(peer) {
            let mut protocols: Vec<ProtocolId> = protocols
                .iter()
                .filter_map(|s| ProtocolId::try_from_str(s))
                .collect();
            protocols.sort();
            if let Some(best_protocol) = protocols.last() {
                self.set_peer_state(peer, PeerState::Responsive(conn_id, *best_protocol));
            }
        }
    }

    pub async fn wantlist_for_peer(&self, peer: &PeerId) -> Vec<Cid> {
        if peer == self.network.self_id() {
            return self.client.get_wantlist().await.into_iter().collect();
        }

        if let Some(ref server) = self.server {
            server.wantlist_for_peer(peer).await
        } else {
            Vec::new()
        }
    }

    fn peer_connected(&self, peer: PeerId) {
        if let Err(err) = self.peers_connected.try_send(peer) {
            warn!(
                "failed to process peer connection from {}: {:?}, dropping",
                peer, err
            );
        }
    }

    fn peer_disconnected(&self, peer: PeerId) {
        if let Err(err) = self.peers_disconnected.try_send(peer) {
            warn!(
                "failed to process peer disconnection from {}: {:?}, dropping",
                peer, err
            );
        }
    }

    fn receive_message(&self, peer: PeerId, message: BitswapMessage) {
        inc!(BitswapMetrics::MessagesReceived);
        record!(BitswapMetrics::MessageBytesIn, message.encoded_len() as u64);
        // TODO: Handle backpressure properly
        if let Err(err) = self.incoming_messages.try_send((peer, message)) {
            warn!(
                "failed to receive message from {}: {:?}, dropping",
                peer, err
            );
        }
    }

    fn get_peer_state(&self, peer: &PeerId) -> Option<PeerState> {
        self.peers.lock().unwrap().get(peer).copied()
    }

    /// Whether the swarm still holds a connection to `peer`.
    fn is_connected(&self, peer: &PeerId) -> bool {
        self.connections
            .lock()
            .unwrap()
            .get(peer)
            .map(|conns| !conns.is_empty())
            .unwrap_or(false)
    }

    /// Hands every waiter on `peer` the outcome of a dial that will not produce
    /// a `ConnectionEstablished` of its own.
    fn resolve_dials(
        &self,
        peer: &PeerId,
        outcome: std::result::Result<Option<ProtocolId>, String>,
    ) {
        let dials = &mut *self.dials.lock().unwrap();
        if let Some(mut dials) = dials.remove(peer) {
            while let Some((id, sender)) = dials.pop() {
                if let Err(err) = sender.send(outcome.clone()) {
                    debug!("dial:{}: failed to send dial response {:?}", id, err)
                }
            }
        }
    }

    /// Points a known peer's record at `connection` without changing whether it
    /// counts as connected or responsive.
    fn refresh_recorded_connection(&self, peer: &PeerId, connection: ConnectionId) {
        let peers = &mut *self.peers.lock().unwrap();
        if let Some(state) = peers.get_mut(peer) {
            *state = match *state {
                PeerState::Responsive(_, protocol) => PeerState::Responsive(connection, protocol),
                _ => PeerState::Connected(connection),
            };
        }
    }

    /// The protocol negotiated with `peer`, if one is known.
    fn negotiated_protocol(&self, peer: &PeerId) -> Option<ProtocolId> {
        match self.get_peer_state(peer) {
            Some(PeerState::Responsive(_, protocol)) => Some(protocol),
            _ => None,
        }
    }

    fn set_peer_state(&self, peer: &PeerId, new_state: PeerState) {
        let peers = &mut *self.peers.lock().unwrap();
        let peer = *peer;
        match peers.entry(peer) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let old_state = *entry.get();
                // skip non state changes
                if old_state == new_state {
                    return;
                }
                // Additional connections go through `refresh_recorded_connection`
                // so they neither strand the peer on the first id nor demote one
                // already known to be responsive.
                if new_state == PeerState::Disconnected {
                    entry.remove();
                } else {
                    *entry.get_mut() = new_state;
                }
                match new_state {
                    PeerState::DialFailure(_) | PeerState::Disconnected => {
                        if old_state.is_connected() {
                            inc!(BitswapMetrics::DisconnectedPeers);
                            self.peer_disconnected(peer);
                        }
                    }
                    PeerState::Connected(_) => {
                        // nothing, just recorded until we receive protocol confirmation
                        inc!(BitswapMetrics::ConnectedPeers);
                    }
                    PeerState::Responsive(_, _) => {
                        inc!(BitswapMetrics::ResponsivePeers);
                        self.peer_connected(peer);
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if new_state != PeerState::Disconnected {
                    entry.insert(new_state);
                }
                match new_state {
                    PeerState::DialFailure(_) | PeerState::Disconnected => {
                        inc!(BitswapMetrics::DisconnectedPeers);
                        self.peer_disconnected(peer);
                    }
                    PeerState::Connected(_) => {
                        inc!(BitswapMetrics::ConnectedPeers);
                    }
                    PeerState::Responsive(_, _) => {
                        inc!(BitswapMetrics::ResponsivePeers);
                        self.peer_connected(peer);
                    }
                }
            }
        }
    }

    fn new_handler(&self) -> BitswapHandler {
        BitswapHandler::new(self.protocol_config.clone(), self.idle_timeout)
    }
}

#[derive(Debug)]
pub enum BitswapEvent {
    /// We have this content, and want it to be provided.
    Provide { key: Cid },
    FindProviders {
        key: Cid,
        response: tokio::sync::mpsc::Sender<std::result::Result<HashSet<PeerId>, String>>,
        limit: usize,
    },
    Ping {
        peer: PeerId,
        response: oneshot::Sender<Option<Duration>>,
    },
}

impl<S: Store> NetworkBehaviour for Bitswap<S> {
    type ConnectionHandler = BitswapHandler;
    type ToSwarm = BitswapEvent;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(self.new_handler())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _addr: &Multiaddr,
        _role_override: libp2p::core::Endpoint,
        _port_use: libp2p::core::transport::PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(self.new_handler())
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(info) => {
                trace!(
                    "connection established {} ({})",
                    info.peer_id,
                    info.other_established
                );
                self.connections
                    .lock()
                    .unwrap()
                    .entry(info.peer_id)
                    .or_default()
                    .insert(info.connection_id);
                if info.other_established == 0 {
                    self.set_peer_state(&info.peer_id, PeerState::Connected(info.connection_id));
                } else {
                    // An additional connection refreshes the recorded id without
                    // re-announcing a peer that is already counted as connected.
                    self.refresh_recorded_connection(&info.peer_id, info.connection_id);
                }
                self.pause_dialing = false;

                // A dial is satisfied the moment the peer is reachable. Waiting
                // for the handler's `Connected` event instead left every dial
                // pending until a bitswap substream negotiated — and nothing in
                // this crate ever emits that event, so no dial ever resolved on
                // success at all.
                self.resolve_dials(&info.peer_id, Ok(self.negotiated_protocol(&info.peer_id)));
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                remaining_established,
                ..
            }) => {
                self.pause_dialing = false;
                {
                    let connections = &mut *self.connections.lock().unwrap();
                    if let Some(conns) = connections.get_mut(&peer_id) {
                        conns.remove(&connection_id);
                        if conns.is_empty() {
                            connections.remove(&peer_id);
                        }
                    }
                }
                if remaining_established == 0 {
                    // Last connection, close it
                    self.set_peer_state(&peer_id, PeerState::Disconnected)
                }
                // While other connections remain the peer stays connected. The
                // recorded id may now name a closed connection, which is why
                // messages are dispatched to any live connection rather than to
                // that id.
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error,
                ..
            }) => {
                // A dial outcome says nothing about connections that already
                // exist. Tearing the peer down here used to leave bitswap's
                // view disagreeing with the swarm's, and because the refused
                // dial is what would have repaired it, that disagreement was
                // permanent.
                let connected = self.is_connected(&peer_id);
                if matches!(error, libp2p::swarm::DialError::Denied { .. }) {
                    self.pause_dialing = true;
                    if !connected {
                        self.set_peer_state(&peer_id, PeerState::Disconnected);
                    }
                } else if !matches!(
                    error,
                    libp2p::swarm::DialError::DialPeerConditionFalse { .. }
                ) && !connected
                {
                    self.set_peer_state(&peer_id, PeerState::DialFailure(Instant::now()));
                }

                trace!("dial_failure {}, {:?}", peer_id, error);
                if connected {
                    // The peer is reachable regardless of what this dial did.
                    self.resolve_dials(&peer_id, Ok(self.negotiated_protocol(&peer_id)));
                } else if matches!(
                    error,
                    libp2p::swarm::DialError::DialPeerConditionFalse { .. }
                ) {
                    // A dial is already in flight — its own
                    // `ConnectionEstablished` will resolve these waiters.
                    // Failing them here would also fail the caller that
                    // started that dial, and any behaviour's refused dial
                    // lands in this arm because `FromSwarm` is broadcast.
                    trace!(
                        "dial to {} already in flight, waiters left pending",
                        peer_id
                    );
                } else {
                    self.resolve_dials(&peer_id, Err(error.to_string()));
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            HandlerEvent::Message {
                mut message,
                protocol,
            } => {
                // mark peer as responsive
                self.set_peer_state(&peer_id, PeerState::Responsive(connection, protocol));

                message.verify_blocks();
                self.receive_message(peer_id, message);
            }
            HandlerEvent::FailedToSendMessage { .. } => {
                // Handle
            }
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        inc!(BitswapMetrics::NetworkBehaviourActionPollTick);
        // limit work
        for _ in 0..50 {
            match Pin::new(&mut self.network).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(ev) => match ev {
                    OutEvent::Disconnect(peer_id, response) => {
                        if let Err(err) = response.send(()) {
                            warn!("failed to send disconnect response {:?}", err)
                        }
                        return Poll::Ready(ToSwarm::CloseConnection {
                            peer_id,
                            connection: CloseConnection::All,
                        });
                    }
                    OutEvent::Dial { peer, response, id } => {
                        match self.get_peer_state(&peer) {
                            Some(PeerState::Responsive(_, protocol_id)) => {
                                // already connected
                                if let Err(err) = response.send(Ok(Some(protocol_id))) {
                                    debug!("dial:{}: failed to send dial response {:?}", id, err)
                                }
                                continue;
                            }
                            Some(PeerState::Connected(_)) => {
                                // already connected
                                if let Err(err) = response.send(Ok(None)) {
                                    debug!("dial:{}: failed to send dial response {:?}", id, err)
                                }
                                continue;
                            }
                            Some(PeerState::DialFailure(dialed))
                                if dialed.elapsed() < DIAL_BACK_OFF =>
                            {
                                // Do not bother trying to dial these for now.
                                debug!("dial:{id}: {peer} is in dial back-off");
                                if let Err(err) =
                                    response.send(Err(format!("dial:{id}: undialable peer")))
                                {
                                    debug!("dial:{id}: failed to send dial response {err:?}")
                                }
                                continue;
                            }
                            _ => {
                                if self.pause_dialing {
                                    debug!("dial:{id}: dialing paused, cannot reach {peer}");
                                    if let Err(err) =
                                        response.send(Err(format!("dial:{id}: dialing paused")))
                                    {
                                        debug!("dial:{id}: failed to send dial response {err:?}",)
                                    }
                                    continue;
                                }

                                self.dials
                                    .lock()
                                    .unwrap()
                                    .entry(peer)
                                    .or_default()
                                    .push((id, response));

                                // Only dial a peer we are not already talking to.
                                // `Always` opened a second connection to a peer
                                // that was already reachable, which peers running
                                // a single-stream pubsub cannot tolerate, and any
                                // redundant dial that failed put this peer into a
                                // ten-minute back-off.
                                return Poll::Ready(ToSwarm::Dial {
                                    opts: DialOpts::peer_id(peer)
                                        .condition(
                                            libp2p::swarm::dial_opts::PeerCondition::DisconnectedAndNotDialing,
                                        )
                                        .build(),
                                });
                            }
                        }
                    }
                    OutEvent::GenerateEvent(ev) => return Poll::Ready(ToSwarm::GenerateEvent(ev)),
                    OutEvent::SendMessage {
                        peer,
                        message,
                        response,
                    } => {
                        tracing::debug!("send message {}", peer);
                        return Poll::Ready(ToSwarm::NotifyHandler {
                            peer_id: peer,
                            handler: NotifyHandler::Any,
                            event: handler::BitswapHandlerIn::Message(message, response),
                        });
                    }
                    OutEvent::ProtectPeer { peer } => {
                        // Keep-alive is per connection, but the recorded id is
                        // the same one that can name a closed connection, and
                        // protecting that leaves the connection actually
                        // carrying bitswap free to be reaped as idle.
                        if matches!(
                            self.get_peer_state(&peer),
                            Some(PeerState::Responsive(_, _))
                        ) {
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id: peer,
                                handler: NotifyHandler::Any,
                                event: handler::BitswapHandlerIn::Protect,
                            });
                        }
                    }
                    OutEvent::UnprotectPeer { peer, response } => {
                        if matches!(
                            self.get_peer_state(&peer),
                            Some(PeerState::Responsive(_, _))
                        ) {
                            let _ = response.send(true);
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id: peer,
                                handler: NotifyHandler::Any,
                                event: handler::BitswapHandlerIn::Unprotect,
                            });
                        }
                        let _ = response.send(false);
                    }
                },
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::anyhow;
    use futures::prelude::*;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::transport::upgrade::Version;
    use libp2p::core::transport::Boxed;
    use libp2p::core::ConnectedPoint;
    use libp2p::identity::Keypair;
    use libp2p::noise;
    use libp2p::swarm::behaviour::ConnectionEstablished;
    use libp2p::swarm::SwarmEvent;
    use libp2p::tcp::{tokio::Transport as TcpTransport, Config as TcpConfig};
    use libp2p::yamux::Config as YamuxConfig;
    use libp2p::{PeerId, Swarm, Transport};
    use tokio::sync::{mpsc, RwLock};
    use tracing::{info, trace};
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    use super::*;
    use crate::Block;

    fn assert_send<T: Send + Sync>() {}

    #[derive(Debug, Clone)]
    struct DummyStore;

    #[async_trait]
    impl Store for DummyStore {
        async fn get_size(&self, _: &Cid) -> Result<usize> {
            todo!()
        }
        async fn get(&self, _: &Cid) -> Result<Block> {
            todo!()
        }
        async fn has(&self, _: &Cid) -> Result<bool> {
            todo!()
        }
    }

    #[test]
    fn test_traits() {
        assert_send::<Bitswap<DummyStore>>();
        assert_send::<&Bitswap<DummyStore>>();
    }

    fn mk_transport() -> (PeerId, Boxed<(PeerId, StreamMuxerBox)>) {
        let local_key = Keypair::generate_ed25519();
        let peer_id = local_key.public().to_peer_id();

        let transport = TcpTransport::new(TcpConfig::default().nodelay(true))
            .upgrade(Version::V1)
            .authenticate(noise::Config::new(&local_key).expect("Noise config creation failed"))
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed();
        (peer_id, transport)
    }

    #[derive(Debug, Clone, Default)]
    struct TestStore {
        store: Arc<RwLock<AHashMap<Cid, Block>>>,
    }

    #[async_trait]
    impl Store for TestStore {
        async fn get_size(&self, cid: &Cid) -> Result<usize> {
            self.store
                .read()
                .await
                .get(cid)
                .map(|block| block.data().len())
                .ok_or_else(|| anyhow!("missing"))
        }

        async fn get(&self, cid: &Cid) -> Result<Block> {
            self.store
                .read()
                .await
                .get(cid)
                .cloned()
                .ok_or_else(|| anyhow!("missing"))
        }

        async fn has(&self, cid: &Cid) -> Result<bool> {
            Ok(self.store.read().await.contains_key(cid))
        }
    }

    #[tokio::test]
    async fn test_get_1_block() {
        get_block::<1>().await;
    }

    #[tokio::test]
    async fn test_get_2_block() {
        get_block::<2>().await;
    }

    #[tokio::test]
    async fn test_get_4_block() {
        get_block::<4>().await;
    }

    #[tokio::test]
    async fn test_get_64_block() {
        get_block::<64>().await;
    }

    #[tokio::test]
    async fn test_get_65_block() {
        get_block::<65>().await;
    }

    #[tokio::test]
    async fn test_get_66_block() {
        get_block::<66>().await;
    }

    #[tokio::test]
    async fn test_get_128_block() {
        tracing_subscriber::registry()
            .with(fmt::layer().pretty())
            .with(EnvFilter::from_default_env())
            .init();

        get_block::<128>().await;
    }

    #[tokio::test]
    async fn test_get_1024_block() {
        get_block::<1024>().await;
    }

    /// The same refusal, observed rather than constructed: a real `Swarm::dial`
    /// under `DisconnectedAndNotDialing` against a peer already connected. The
    /// peer must survive it and still serve blocks, which is what breaks when a
    /// refusal is treated as the peer going away.
    #[tokio::test]
    async fn test_get_block_after_a_refused_redial() {
        let (peer1_id, trans) = mk_transport();
        let store1 = TestStore::default();
        let bs1 = Bitswap::new(peer1_id, store1.clone(), Config::default()).await;
        let mut swarm1 = Swarm::new(
            trans,
            bs1,
            peer1_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );
        let block = create_random_block_v1();
        store1
            .store
            .write()
            .await
            .insert(*block.cid(), block.clone());

        let (tx, mut rx) = mpsc::channel::<Multiaddr>(1);
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        let peer1 = tokio::task::spawn(async move {
            while swarm1.next().now_or_never().is_some() {}
            let listeners: Vec<_> = Swarm::listeners(&swarm1).collect();
            for l in listeners {
                tx.send(l.clone()).await.unwrap();
            }
            loop {
                let ev = swarm1.next().await;
                trace!("peer1: {:?}", ev);
            }
        });

        let (peer2_id, trans) = mk_transport();
        let bs2 = Bitswap::new(peer2_id, TestStore::default(), Config::default()).await;
        let mut swarm2 = Swarm::new(
            trans,
            bs2,
            peer2_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );
        let swarm2_bs = swarm2.behaviour().clone();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let peer2 = tokio::task::spawn(async move {
            let addr = rx.recv().await.unwrap();
            Swarm::dial(&mut swarm2, addr).unwrap();

            let mut ready_tx = Some(ready_tx);
            loop {
                match swarm2.next().await {
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        swarm2.behaviour().on_identify(
                            &peer_id,
                            &[
                                "/ipfs/bitswap/1.2.0".to_string(),
                                "/ipfs/bitswap/1.1.0".to_string(),
                            ],
                        );
                        // The swarm refuses this and reports it to every
                        // behaviour as a `DialFailure` before it returns.
                        let refused = Swarm::dial(
                            &mut swarm2,
                            DialOpts::peer_id(peer_id)
                                .condition(
                                    libp2p::swarm::dial_opts::PeerCondition::DisconnectedAndNotDialing,
                                )
                                .build(),
                        );
                        assert!(
                            matches!(
                                refused,
                                Err(libp2p::swarm::DialError::DialPeerConditionFalse(_))
                            ),
                            "expected the redial to be refused, got {refused:?}"
                        );
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    ev => trace!("peer2: {:?}", ev),
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(30), ready_rx)
            .await
            .expect("peer2 never connected")
            .unwrap();

        let received = tokio::time::timeout(
            Duration::from_secs(30),
            swarm2_bs.client().get_block(block.cid()),
        )
        .await
        .expect("fetch never completed: a refused redial took the peer down")
        .unwrap();
        assert_eq!(block, received);

        peer1.abort();
        peer2.abort();
    }

    /// A dial refused because a connection already exists must not fail the
    /// waiters of the dial that is still in flight. The refusal is delivered
    /// synchronously by `Swarm::dial`, and any behaviour's refused dial reaches
    /// bitswap because `FromSwarm` is broadcast, so failing waiters here breaks
    /// callers that never asked for the refused dial.
    #[tokio::test]
    async fn refused_dial_leaves_in_flight_waiters_pending() {
        let (self_id, _) = mk_transport();
        let bs = Bitswap::new(self_id, TestStore::default(), Config::default()).await;
        let peer = PeerId::random();

        let (first_tx, mut first_rx) = oneshot::channel();
        let (second_tx, mut second_rx) = oneshot::channel();
        bs.dials
            .lock()
            .unwrap()
            .insert(peer, vec![(1, first_tx), (2, second_tx)]);

        let mut bs = bs;
        let error = libp2p::swarm::DialError::DialPeerConditionFalse(
            libp2p::swarm::dial_opts::PeerCondition::DisconnectedAndNotDialing,
        );
        bs.on_swarm_event(FromSwarm::DialFailure(DialFailure {
            peer_id: Some(peer),
            error: &error,
            connection_id: ConnectionId::new_unchecked(1),
        }));

        assert!(
            matches!(
                first_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "the in-flight dial's own waiter was resolved by an unrelated refusal"
        );
        assert!(matches!(
            second_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // The dial that was actually in flight now lands, and resolves both.
        let endpoint = ConnectedPoint::Dialer {
            address: "/ip4/127.0.0.1/tcp/1".parse().unwrap(),
            role_override: libp2p::core::Endpoint::Dialer,
            port_use: libp2p::core::transport::PortUse::New,
        };
        bs.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
            peer_id: peer,
            connection_id: ConnectionId::new_unchecked(1),
            endpoint: &endpoint,
            failed_addresses: &[],
            other_established: 0,
        }));

        assert!(first_rx.try_recv().unwrap().is_ok());
        assert!(second_rx.try_recv().unwrap().is_ok());
    }

    /// A denied dial says nothing about connections that already exist. Dropping
    /// the peer here left bitswap's view disagreeing with the swarm's, and since
    /// the refused dial is what would have repaired it, the peer stayed
    /// unreachable for good.
    #[tokio::test]
    async fn denied_dial_keeps_a_peer_that_is_still_connected() {
        let (self_id, _) = mk_transport();
        let bs = Bitswap::new(self_id, TestStore::default(), Config::default()).await;
        let peer = PeerId::random();
        let mut bs = bs;

        let endpoint = ConnectedPoint::Dialer {
            address: "/ip4/127.0.0.1/tcp/1".parse().unwrap(),
            role_override: libp2p::core::Endpoint::Dialer,
            port_use: libp2p::core::transport::PortUse::New,
        };
        bs.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
            peer_id: peer,
            connection_id: ConnectionId::new_unchecked(1),
            endpoint: &endpoint,
            failed_addresses: &[],
            other_established: 0,
        }));
        assert!(bs.get_peer_state(&peer).is_some());

        let error = libp2p::swarm::DialError::Denied {
            cause: libp2p::swarm::ConnectionDenied::new(Error::new(ErrorKind::Other, "limit")),
        };
        bs.on_swarm_event(FromSwarm::DialFailure(DialFailure {
            peer_id: Some(peer),
            error: &error,
            connection_id: ConnectionId::new_unchecked(2),
        }));

        assert!(
            bs.is_connected(&peer),
            "a denied extra dial dropped a peer that is still connected"
        );
        assert!(
            bs.get_peer_state(&peer).is_some(),
            "peer state was cleared while a connection remains, and nothing repairs it"
        );

        // A later dial resolves from the surviving connection rather than being
        // refused forever.
        let (tx, mut rx) = oneshot::channel();
        bs.dials.lock().unwrap().insert(peer, vec![(3, tx)]);
        bs.resolve_dials(&peer, Ok(bs.negotiated_protocol(&peer)));
        assert!(rx.try_recv().unwrap().is_ok());
    }

    /// Two connections to one peer, then the connection bitswap most recently
    /// recorded closes while the other stays up. The peer is still reachable, so
    /// the fetch must still complete. Addressing messages to the recorded
    /// connection id instead dropped every one of them, and the peer never
    /// recovered because a surviving connection raises no swarm event.
    #[tokio::test]
    async fn test_get_block_after_recorded_connection_closes() {
        let (peer1_id, trans) = mk_transport();
        let store1 = TestStore::default();
        let bs1 = Bitswap::new(peer1_id, store1.clone(), Config::default()).await;
        let mut swarm1 = Swarm::new(
            trans,
            bs1,
            peer1_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );
        let block = create_random_block_v1();
        store1
            .store
            .write()
            .await
            .insert(*block.cid(), block.clone());

        let (tx, mut rx) = mpsc::channel::<Multiaddr>(1);
        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
        let peer1 = tokio::task::spawn(async move {
            while swarm1.next().now_or_never().is_some() {}
            let listeners: Vec<_> = Swarm::listeners(&swarm1).collect();
            for l in listeners {
                tx.send(l.clone()).await.unwrap();
            }
            loop {
                let ev = swarm1.next().await;
                trace!("peer1: {:?}", ev);
            }
        });

        let (peer2_id, trans) = mk_transport();
        let store2 = TestStore::default();
        let bs2 = Bitswap::new(peer2_id, store2.clone(), Config::default()).await;
        let mut swarm2 = Swarm::new(
            trans,
            bs2,
            peer2_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );
        let swarm2_bs = swarm2.behaviour().clone();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let peer2 = tokio::task::spawn(async move {
            let addr = rx.recv().await.unwrap();
            Swarm::dial(&mut swarm2, addr.clone()).unwrap();
            Swarm::dial(&mut swarm2, addr).unwrap();

            let mut established = Vec::new();
            let mut ready_tx = Some(ready_tx);
            loop {
                match swarm2.next().await {
                    Some(SwarmEvent::ConnectionEstablished {
                        peer_id,
                        connection_id,
                        ..
                    }) => {
                        swarm2.behaviour().on_identify(
                            &peer_id,
                            &[
                                "/ipfs/bitswap/1.2.0".to_string(),
                                "/ipfs/bitswap/1.1.0".to_string(),
                            ],
                        );
                        established.push(connection_id);
                        if established.len() == 2 {
                            swarm2.close_connection(established[1]);
                        }
                    }
                    Some(SwarmEvent::ConnectionClosed { .. }) => {
                        if let Some(tx) = ready_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                    ev => trace!("peer2: {:?}", ev),
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(30), ready_rx)
            .await
            .expect("the recorded connection never closed")
            .unwrap();

        let received = tokio::time::timeout(
            Duration::from_secs(30),
            swarm2_bs.client().get_block(block.cid()),
        )
        .await
        .expect("fetch never completed: bitswap is stranded on the closed connection")
        .unwrap();
        assert_eq!(block, received);

        peer1.abort();
        peer2.abort();
    }

    async fn get_block<const N: usize>() {
        let (peer1_id, trans) = mk_transport();
        let store1 = TestStore::default();
        let bs1 = Bitswap::new(peer1_id, store1.clone(), Config::default()).await;
        let mut swarm1 = Swarm::new(
            trans,
            bs1,
            peer1_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );
        let blocks = (0..N).map(|_| create_random_block_v1()).collect::<Vec<_>>();

        for block in &blocks {
            store1
                .store
                .write()
                .await
                .insert(*block.cid(), block.clone());
        }

        let (tx, mut rx) = mpsc::channel::<Multiaddr>(1);

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let peer1 = tokio::task::spawn(async move {
            while swarm1.next().now_or_never().is_some() {}
            let listeners: Vec<_> = Swarm::listeners(&swarm1).collect();
            for l in listeners {
                tx.send(l.clone()).await.unwrap();
            }

            loop {
                let ev = swarm1.next().await;
                trace!("peer1: {:?}", ev);
            }
        });

        info!("peer2: startup");
        let (peer2_id, trans) = mk_transport();
        let store2 = TestStore::default();
        let bs2 = Bitswap::new(peer2_id, store2.clone(), Config::default()).await;

        let mut swarm2 = Swarm::new(
            trans,
            bs2,
            peer2_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );

        let swarm2_bs = swarm2.behaviour().clone();
        let peer2 = tokio::task::spawn(async move {
            let addr = rx.recv().await.unwrap();
            info!("peer2: dialing peer1 at {}", addr);
            Swarm::dial(&mut swarm2, addr).unwrap();

            loop {
                match swarm2.next().await {
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        trace!("peer2: connected to {}", peer_id);
                        // simulate identify to inform bitswap about the protocols
                        swarm2.behaviour().on_identify(
                            &peer_id,
                            &[
                                "/ipfs/bitswap/1.2.0".to_string(),
                                "/ipfs/bitswap/1.1.0".to_string(),
                            ],
                        );
                    }
                    ev => trace!("peer2: {:?}", ev),
                }
            }
        });

        {
            info!("peer2: fetching block - ordered");
            let blocks = blocks.clone();
            let mut futs = Vec::new();
            for block in &blocks {
                let client = swarm2_bs.client().clone();
                futs.push(async move {
                    // Should work, because retrieved
                    let received_block = client.get_block(block.cid()).await?;

                    info!("peer2: received block");
                    Ok::<Block, anyhow::Error>(received_block)
                });
            }

            let results = futures::future::join_all(futs).await;
            for (block, result) in blocks.into_iter().zip(results) {
                let received_block = result.unwrap();
                assert_eq!(block, received_block);
            }
        }

        {
            info!("peer2: fetching block - unordered");
            let mut blocks = blocks.clone();
            let futs = futures::stream::FuturesUnordered::new();
            for block in &blocks {
                let client = swarm2_bs.client().clone();
                futs.push(async move {
                    // Should work, because retrieved
                    let received_block = client.get_block(block.cid()).await?;

                    info!("peer2: received block");
                    Ok::<Block, anyhow::Error>(received_block)
                });
            }

            let mut results = futs.try_collect::<Vec<_>>().await.unwrap();
            results.sort();
            blocks.sort();
            for (block, received_block) in blocks.into_iter().zip(results) {
                assert_eq!(block, received_block);
            }
        }

        {
            info!("peer2: fetching block - session");
            let mut blocks = blocks.clone();
            let ids: Vec<_> = blocks.iter().map(|b| *b.cid()).collect();
            let session = swarm2_bs.client().new_session().await;
            let (blocks_receiver, _guard) = session.get_blocks(&ids).await.unwrap().into_parts();
            let mut results: Vec<_> = blocks_receiver.collect().await;

            results.sort();
            blocks.sort();
            for (block, received_block) in blocks.into_iter().zip(results) {
                assert_eq!(block, received_block);
            }
        }

        info!("--shutting down peer1");
        peer1.abort();
        peer1.await.ok();

        info!("--shutting down peer2");
        peer2.abort();
        peer2.await.ok();
    }
}
