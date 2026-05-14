pub(crate) mod bitswap {
    use super::core::MRecorder;

    #[derive(Debug, Copy, Clone)]
    #[allow(dead_code)]
    pub enum BitswapMetrics {
        RequestsTotal,
        CanceledTotal,
        SentBlockBytes,
        ReceivedBlockBytes,
        MessageBytesOut,
        MessageBytesIn,
        BlocksIn,
        BlocksOut,
        ProvidersTotal,
        AttemptedDials,
        Dials,
        KnownPeers,
        ForgottenPeers,
        WantedBlocks,
        WantedBlocksReceived,
        WantHaveBlocks,
        CancelBlocks,
        CancelWantBlocks,
        ConnectedPeers,
        ResponsivePeers,
        UnresponsivePeers,
        DisconnectedPeers,
        MessagesAttempted,
        MessagesSent,
        MessagesProcessingClient,
        MessagesProcessingServer,
        MessagesReceived,
        EventsBackpressureIn,
        EventsBackpressureOut,
        PollActionConnectedWants,
        PollActionConnected,
        PollActionNotConnected,
        ProtocolUnsupported,
        HandlerPollCount,
        HandlerPollEventCount,
        HandlerConnUpgradeErrors,
        InboundSubstreamsCreatedLimit,
        OutboundSubstreamsEvent,
        OutboundSubstreamsCreatedLimit,
        HandlerInboundLoopCount,
        HandlerOutboundLoopCount,
        SessionsCreated,
        SessionsDestroyed,
        ProviderQueryCreated,
        ProviderQuerySuccess,
        ProviderQueryError,
        EngineActiveTasks,
        EnginePendingTasks,
        ClientLoopTick,
        ServerTaskLoopTick,
        ServerProviderTaskLoopTick,
        ServerKeyProviderTaskLoopTick,
        MessageQueueWorkerLoopTick,
        SessionLoopTick,
        SessionGetBlockLoopTick,
        FindMorePeersLoopTick,
        DontHaveTimeoutLoopTick,
        SessionWantSenderLoopTick,
        EngineLoopTick,
        ScoreLedgerLoopTick,
        PeerManagerLoopTick,
        MessageQueuesCreated,
        MessageQueuesDestroyed,
        MessageQueuesStopped,
        NetworkBehaviourActionPollTick,
        NetworkPollTick,
    }

    impl MRecorder for BitswapMetrics {
        fn record(&self, _value: u64) {}
    }
}

pub(crate) mod core {
    pub trait MRecorder {
        fn record(&self, value: u64);
    }
}

#[allow(dead_code)]
pub(crate) mod config {
    #[derive(Debug, Clone, Default)]
    pub struct Config {
        pub service_name: String,
        pub build: String,
        pub version: String,
    }

    impl Config {
        pub fn with_service_name(mut self, name: String) -> Self {
            self.service_name = name;
            self
        }

        pub fn with_build(mut self, build: String) -> Self {
            self.build = build;
            self
        }

        pub fn with_version(mut self, version: String) -> Self {
            self.version = version;
            self
        }
    }
}

#[macro_export]
macro_rules! record {
    ($metric:expr, $value:expr) => {
        $metric.record($value);
    };
}

#[macro_export]
macro_rules! inc {
    ($metric:expr) => {
        $metric.record(1);
    };
}

pub(crate) use crate::{inc, record};
