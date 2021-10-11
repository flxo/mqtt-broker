use std::iter;

use async_trait::async_trait;
use mqtt_v5::types::{
    ConnectPacket, ConnectReason, PublishAckReason, PublishPacket, PublishReceivedReason,
    SubscribeAckReason, SubscribePacket,
};

/// Result of publish interception. Either accept or reject the publication. The reject
/// contains a ConnectReason depending on the QOS level of the publication.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectResult {
    Accept,
    Reject(ConnectReason),
}

/// Result of a publish interception. Return PublishAck or PubRec reason
/// code that shall be sent to the client.
#[derive(Debug, Clone, PartialEq)]
pub enum PublishRejectReason {
    PublishAckReason(PublishAckReason),
    PublishReceivedReason(PublishReceivedReason),
}

/// Result of of a publication interception. `Accept` means that the publication should be
/// processes according to the MQTT standard. `Reject` means that the publication shall be discarded
/// and the client shall be notified with the specified reason.
#[derive(Debug, Clone, PartialEq)]
pub enum PublishResult {
    Accept,
    Rejected(PublishRejectReason),
}

/// Result of a subscribe interception. `Accept` means that the subscription should be processed and
/// added to the client session.
#[derive(Debug, Clone, PartialEq)]
pub enum SubscribeResult {
    Accept,
    Reject(SubscribeAckReason),
}

/// Interceptor plugged into a broker for custom behavior on certain packets.
#[async_trait]
pub trait Interceptor {
    async fn connect(&mut self, _packet: &ConnectPacket) -> ConnectResult {
        ConnectResult::Accept
    }

    async fn publish(&mut self, _client_id: &str, _packet: &PublishPacket) -> PublishResult {
        PublishResult::Accept
    }

    async fn subscribe(
        &mut self,
        _client_id: &str,
        packet: &SubscribePacket,
    ) -> Vec<SubscribeResult> {
        iter::repeat(SubscribeResult::Accept).take(packet.subscription_topics.len()).collect()
    }
}

impl Interceptor for () {}
