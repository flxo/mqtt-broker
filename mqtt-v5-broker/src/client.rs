use crate::broker::{BrokerMessage, WillDisconnectLogic};
use futures::{
    future::{self, select, Either},
    stream, Sink, SinkExt, Stream, StreamExt,
};
use log::{debug, info, trace, warn};
use mqtt_v5::types::{
    DecodeError, DisconnectPacket, DisconnectReason, EncodeError, Packet, ProtocolError,
    ProtocolVersion, QoS,
};
use nanoid::nanoid;
use std::{marker::Unpin, time::Duration};
use tokio::{
    pin,
    sync::mpsc::{self, Receiver, Sender},
    task, time,
};

type PacketResult = Result<Packet, DecodeError>;

/// Timeout when writing to a client sink
const SINK_SEND_TIMEOUT: Duration = Duration::from_secs(1);

pub struct UnconnectedClient<ST, SI>
where
    ST: Stream<Item = PacketResult> + Send + Sync + 'static,
    SI: Sink<Packet, Error = EncodeError> + Send + Sync + 'static,
{
    packet_stream: ST,
    packet_sink: SI,
    broker_tx: Sender<BrokerMessage>,
}

impl<ST, SI> UnconnectedClient<ST, SI>
where
    ST: Stream<Item = PacketResult> + Unpin + Send + Sync + 'static,
    SI: Sink<Packet, Error = EncodeError> + Send + Sync + 'static,
{
    pub fn new(packet_stream: ST, packet_sink: SI, broker_tx: Sender<BrokerMessage>) -> Self {
        Self { packet_stream, packet_sink, broker_tx }
    }

    pub async fn handshake(mut self) -> Result<Client<ST, SI>, ProtocolError> {
        let first_packet = time::timeout(Duration::from_secs(2), self.packet_stream.next())
            .await
            .map_err(|_| ProtocolError::ConnectTimedOut)?;

        trace!("First packet: {:#?}", first_packet);

        match first_packet {
            Some(Ok(Packet::Connect(mut connect_packet))) => {
                let protocol_version = connect_packet.protocol_version;

                if connect_packet.protocol_name != "MQTT" {
                    if protocol_version == ProtocolVersion::V500 {
                        // TODO(bschwind) - Respond to the client with
                        //                  0x84 (Unsupported Protocol Version)
                    }

                    return Err(ProtocolError::InvalidProtocolName);
                }

                let (sender, receiver) = mpsc::channel(5);

                if connect_packet.client_id.is_empty() {
                    connect_packet.client_id = nanoid!();
                }

                let client_id = connect_packet.client_id.clone();
                let keepalive_seconds = if connect_packet.keep_alive == 0 {
                    None
                } else {
                    Some(connect_packet.keep_alive)
                };

                let self_tx = sender.clone();

                self.broker_tx
                    .send(BrokerMessage::NewClient(Box::new(connect_packet), sender))
                    .await
                    .expect("Couldn't send NewClient message to broker");

                Ok(Client::new(
                    client_id,
                    protocol_version,
                    keepalive_seconds,
                    self.packet_stream,
                    self.packet_sink,
                    self.broker_tx,
                    receiver,
                    self_tx,
                ))
            },
            Some(Ok(_)) => Err(ProtocolError::FirstPacketNotConnect),
            Some(Err(e)) => Err(ProtocolError::MalformedPacket(e)),
            None => {
                // TODO(bschwind) - Technically end of stream?
                Err(ProtocolError::FirstPacketNotConnect)
            },
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq)]
pub enum ClientMessage {
    Packet(Packet),
    Packets(Vec<Packet>),
    Disconnect(DisconnectReason),
}

pub struct Client<ST, SI>
where
    ST: Stream<Item = PacketResult>,
    SI: Sink<Packet, Error = EncodeError>,
{
    id: String,
    _protocol_version: ProtocolVersion,
    keepalive_seconds: Option<u16>,
    packet_stream: ST,
    packet_sink: SI,
    broker_tx: Sender<BrokerMessage>,
    broker_rx: Receiver<ClientMessage>,
    self_tx: Sender<ClientMessage>,
}

impl<ST, SI> Client<ST, SI>
where
    ST: Stream<Item = PacketResult> + Unpin + Send + Sync + 'static,
    SI: Sink<Packet, Error = EncodeError> + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        protocol_version: ProtocolVersion,
        keepalive_seconds: Option<u16>,
        packet_stream: ST,
        packet_sink: SI,
        broker_tx: Sender<BrokerMessage>,
        broker_rx: Receiver<ClientMessage>,
        self_tx: Sender<ClientMessage>,
    ) -> Self {
        Self {
            id,
            _protocol_version: protocol_version,
            keepalive_seconds,
            packet_stream,
            packet_sink,
            broker_tx,
            broker_rx,
            self_tx,
        }
    }

    async fn handle_socket_reads(
        mut stream: ST,
        client_id: String,
        keepalive_seconds: Option<u16>,
        broker_tx: Sender<BrokerMessage>,
        self_tx: Sender<ClientMessage>,
    ) {
        // The keepalive should be 1.5 times the specified keepalive value in the connect packet.
        let keepalive_duration = keepalive_seconds
            .into_iter()
            .map(|k| ((k as f32) * 1.5) as u64)
            .map(Duration::from_secs)
            .next();

        loop {
            let next_packet = {
                if let Some(keepalive_duration) = keepalive_duration {
                    let next_packet = time::timeout(keepalive_duration, stream.next())
                        .await
                        .map_err(|_| ProtocolError::KeepAliveTimeout);

                    if let Err(ProtocolError::KeepAliveTimeout) = next_packet {
                        break;
                    }

                    next_packet.unwrap()
                } else {
                    stream.next().await
                }
            };

            match next_packet {
                Some(Ok(frame)) => {
                    trace!("{}: Processing packet {:#?}", client_id, frame);
                    match frame {
                        Packet::Subscribe(packet) => {
                            broker_tx
                                .send(BrokerMessage::Subscribe(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send Subscribe message to broker");
                        },
                        Packet::Unsubscribe(packet) => {
                            broker_tx
                                .send(BrokerMessage::Unsubscribe(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send Unsubscribe message to broker");
                        },
                        Packet::Publish(packet) => {
                            match packet.qos {
                                QoS::AtMostOnce => {},
                                QoS::AtLeastOnce | QoS::ExactlyOnce => {
                                    assert!(
                                        packet.packet_id.is_some(),
                                        "Packets with QoS 1&2 need packet identifiers"
                                    );
                                },
                            }

                            broker_tx
                                .send(BrokerMessage::Publish(client_id.clone(), Box::new(packet)))
                                .await
                                .expect("Couldn't send Publish message to broker");
                        },
                        Packet::PublishAck(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishAck(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishAck message to broker");
                        },
                        Packet::PublishRelease(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishRelease(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishRelease message to broker");
                        },
                        Packet::PublishReceived(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishReceived(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishReceive message to broker");
                        },
                        Packet::PublishComplete(packet) => {
                            broker_tx
                                .send(BrokerMessage::PublishComplete(client_id.clone(), packet))
                                .await
                                .expect("Couldn't send PublishCompelte message to broker");
                        },
                        Packet::PingRequest => {
                            self_tx
                                .send(ClientMessage::Packet(Packet::PingResponse))
                                .await
                                .expect("Couldn't send PingResponse message to self");
                        },
                        Packet::Disconnect(packet) => {
                            let will_disconnect_logic =
                                if packet.reason_code == DisconnectReason::NormalDisconnection {
                                    WillDisconnectLogic::DoNotSend
                                } else {
                                    WillDisconnectLogic::Send
                                };

                            broker_tx
                                .send(BrokerMessage::Disconnect(
                                    client_id.clone(),
                                    will_disconnect_logic,
                                ))
                                .await
                                .expect("Couldn't send Disconnect message to broker");

                            return;
                        },
                        p => warn!("Discarding unimplemented packet: {:#?}", p),
                    }
                },
                Some(Err(err)) => {
                    warn!("{}: Error while reading frame: {:?}", client_id, err);
                    break;
                },
                None => {
                    info!("{}: Disconnected", client_id);
                    break;
                },
            }
        }

        broker_tx
            .send(BrokerMessage::Disconnect(client_id.clone(), WillDisconnectLogic::Send))
            .await
            .expect("Couldn't send Disconnect message to broker");
    }

    async fn handle_socket_writes(
        sink: SI,
        client_id: String,
        mut broker_rx: Receiver<ClientMessage>,
    ) {
        pin!(sink);

        while let Some(frame) = broker_rx.recv().await {
            let mut packets = match frame {
                ClientMessage::Packets(packets) => Either::Left(stream::iter(packets)),
                ClientMessage::Packet(packet) => Either::Right(stream::once(future::ready(packet))),
                ClientMessage::Disconnect(reason_code) => {
                    let disconnect_packet = DisconnectPacket {
                        reason_code,
                        session_expiry_interval: None,
                        reason_string: None,
                        user_properties: vec![],
                        server_reference: None,
                    };

                    if let Err(e) = sink.send(Packet::Disconnect(disconnect_packet)).await {
                        warn!(
                            "{}: Failed to send disconnect packet to framed socket: {:?}",
                            client_id, e
                        );
                    }

                    info!("{}: Broker told the client to disconnect", client_id);

                    return;
                },
            };

            // Process each packet in a dedicated timeout to be fair
            while let Some(packet) = packets.next().await {
                trace!("{}: Sending packet {:#?}", client_id, packet);
                let send = sink.send(packet);
                match time::timeout(SINK_SEND_TIMEOUT, send).await {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => {
                        warn!("{}: Failed to write to client client socket: {:?}", client_id, e);
                        return;
                    },
                    Err(_) => {
                        warn!("{}: Timeout during client socket write. Disconnecting", client_id);
                        return;
                    },
                }
            }
        }
    }

    pub async fn run(self) {
        let Client {
            id,
            _protocol_version,
            keepalive_seconds,
            packet_stream,
            packet_sink,
            broker_tx,
            broker_rx,
            self_tx,
        } = self;

        let task_rx = task::spawn(Self::handle_socket_reads(
            packet_stream,
            id.clone(),
            keepalive_seconds,
            broker_tx,
            self_tx,
        ));
        let task_tx = task::spawn(Self::handle_socket_writes(packet_sink, id.clone(), broker_rx));

        select(task_rx, task_tx).await;
        debug!("Client ID {} task exit", id);
    }
}
