use crate::{broker::BrokerMessage, client::UnconnectedClient};
use bytes::BytesMut;
use futures::{stream, SinkExt, StreamExt};
use log::{debug, info, warn};
use mqtt_v5::{
    codec::MqttCodec,
    encoder,
    types::{DecodeError, EncodeError, Packet, ProtocolVersion},
    websocket::{
        codec::{Message, MessageCodec as WsMessageCodec, Opcode},
        WsUpgraderCodec,
    },
};
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    net::{TcpListener, UnixListener},
    sync::mpsc::Sender,
};
use tokio_util::codec::Framed;

async fn client_handler<I: AsyncRead + AsyncWrite>(stream: I, broker_tx: Sender<BrokerMessage>) {
    debug!("Handling a client");

    let (sink, stream) = Framed::new(stream, MqttCodec::new()).split();
    let unconnected_client = UnconnectedClient::new(stream, sink, broker_tx);

    let connected_client = match unconnected_client.handshake().await {
        Ok(connected_client) => connected_client,
        Err(err) => {
            warn!("Protocol error during connection handshake: {:?}", err);
            return;
        },
    };

    connected_client.run().await;
}

async fn upgrade_stream<I: AsyncRead + AsyncWrite + Unpin>(stream: I) -> Framed<I, WsMessageCodec> {
    let mut upgrade_framed = Framed::new(stream, WsUpgraderCodec::new());

    let upgrade_msg = upgrade_framed.next().await;

    if let Some(Ok(websocket_key)) = upgrade_msg {
        let _ = upgrade_framed.send(websocket_key).await;
    }

    let old_parts = upgrade_framed.into_parts();
    let mut new_parts =
        Framed::new(old_parts.io, WsMessageCodec::with_masked_encode(false)).into_parts();
    new_parts.read_buf = old_parts.read_buf;
    new_parts.write_buf = old_parts.write_buf;

    Framed::from_parts(new_parts)
}

async fn websocket_client_handler<I: AsyncRead + AsyncWrite + Unpin>(
    stream: I,
    broker_tx: Sender<BrokerMessage>,
) {
    debug!("Handling a WebSocket client");

    let ws_framed = upgrade_stream(stream).await;

    let (sink, ws_stream) = ws_framed.split();
    let sink = sink.with(|packet: Packet| {
        let mut payload_bytes = BytesMut::new();
        // TODO(bschwind) - Support MQTTv5 here. With a stateful Framed object we can store
        //                  the version on a successful Connect decode, but in this code structure
        //                  we can't pass state from the stream to the sink.
        encoder::encode_mqtt(&packet, &mut payload_bytes, ProtocolVersion::V311);

        async {
            let result: Result<Message, EncodeError> = Ok(Message::binary(payload_bytes.freeze()));
            result
        }
    });

    let read_buf = BytesMut::with_capacity(4096);

    let stream = stream::unfold(
        (ws_stream, read_buf, ProtocolVersion::V311),
        |(mut ws_stream, mut read_buf, mut protocol_version)| {
            async move {
                // Loop until we've built up enough data from the WebSocket stream
                // to decode a new MQTT packet
                loop {
                    // Try to read an MQTT packet from the read buffer
                    match mqtt_v5::decoder::decode_mqtt(&mut read_buf, protocol_version) {
                        Ok(Some(packet)) => {
                            if let Packet::Connect(packet) = &packet {
                                protocol_version = packet.protocol_version;
                            }

                            // If we got one, return it
                            return Some((Ok(packet), (ws_stream, read_buf, protocol_version)));
                        },
                        Err(e) => {
                            // If we had a decode error, propagate the error along the stream
                            return Some((Err(e), (ws_stream, read_buf, protocol_version)));
                        },
                        Ok(None) => {
                            // Otherwise we need more binary data from the WebSocket stream
                        },
                    }

                    let ws_frame = ws_stream.next().await;

                    match ws_frame {
                        Some(Ok(message)) => {
                            if message.opcode() == Opcode::Close {
                                return None;
                            }

                            if message.opcode() == Opcode::Ping {
                                debug!("Got a websocket ping");
                            }

                            if message.opcode() != Opcode::Binary {
                                // MQTT Control Packets MUST be sent in WebSocket binary data frames
                                return Some((
                                    Err(DecodeError::BadTransport),
                                    (ws_stream, read_buf, protocol_version),
                                ));
                            }

                            read_buf.extend_from_slice(&message.into_data());
                        },
                        Some(Err(e)) => {
                            warn!("Error while reading from WebSocket stream: {:?}", e);
                            // If we had a decode error in the WebSocket layer,
                            // propagate the it along the stream
                            return Some((
                                Err(DecodeError::BadTransport),
                                (ws_stream, read_buf, protocol_version),
                            ));
                        },
                        None => {
                            // The WebSocket stream is over, so we are too
                            return None;
                        },
                    }
                }
            }
        },
    );

    tokio::pin!(stream);
    let unconnected_client = UnconnectedClient::new(stream, sink, broker_tx);

    let connected_client = match unconnected_client.handshake().await {
        Ok(connected_client) => connected_client,
        Err(err) => {
            warn!("Protocol error during connection handshake: {:?}", err);
            return;
        },
    };

    connected_client.run().await;
}

pub async fn listen(addr: &str, broker_tx: Sender<BrokerMessage>) -> io::Result<()> {
    let addr = url::Url::parse(addr).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    info!("Listening on {}", addr);

    match addr.scheme() {
        "unix" => {
            let listener = UnixListener::bind(&addr.path())?;
            loop {
                let (stream, client_addr) = listener.accept().await?;
                debug!("New unix connection from {:?} on {}", client_addr, addr);
                tokio::spawn(client_handler(stream, broker_tx.clone()));
            }
        },
        "wss" | "tcp" => {
            let socket_addr = addr
                .socket_addrs(|| match addr.scheme() {
                    "tcp" => Some(1883),
                    "wss" => Some(8080),
                    _ => None,
                })
                .and_then(|addrs| {
                    addrs
                        .first()
                        .cloned()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to resolve"))
                })?;

            let listener = TcpListener::bind(socket_addr).await?;

            loop {
                let (stream, client_addr) = listener.accept().await?;
                debug!("New tcp connection from {} on {}", client_addr, addr);
                match addr.scheme() {
                    "tcp" => drop(tokio::spawn(client_handler(stream, broker_tx.clone()))),
                    "wss" => {
                        drop(tokio::spawn(websocket_client_handler(stream, broker_tx.clone())))
                    },
                    _ => unreachable!(),
                }
            }
        },
        _ => unimplemented!("Unimplemented scheme {}", addr.scheme()),
    }
}
