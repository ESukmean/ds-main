use bytes::{Buf, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::packer::{ControlPacket, Packer, Packet};

pub struct BroadcasterMangement {
    video_rx: tokio::sync::broadcast::Receiver<Bytes>,
}
impl BroadcasterMangement {
    pub fn new(video_rx: tokio::sync::broadcast::Receiver<Bytes>) -> Self {
        Self { video_rx }
    }

    pub async fn run_server(mut self, port: u16) {
        let (tx_control, mut rx_control) = tokio::sync::mpsc::channel(1024);
        let (tx_brod, _rx_brod) = tokio::sync::broadcast::channel::<Bytes>(1024);
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .unwrap();

        loop {
            tokio::select! {
                accept = listener.accept() => {
                    if accept.is_err() {
                        continue;
                    }

                    let (sock, _) = accept.unwrap();
                    println!("connection created");
                    tokio::spawn(Broadcaster::new(sock, tx_brod.subscribe(), tx_control.clone()).run_server());
                },
                Some(data) = rx_control.recv() => {
                    println!("recv - rx_control");
                    tx_brod.send(Packer::<Packet>::pack(Packet::Control(data)));
                }
                Ok(video) = self.video_rx.recv() => {
                    tx_brod.send(Packer::<Packet>::pack(Packet::Video(video)));
                }
            }
        }
    }
}
pub struct Broadcaster {
    sock: TcpStream,
    rx_data: tokio::sync::broadcast::Receiver<Bytes>,
    tx_control: tokio::sync::mpsc::Sender<ControlPacket>,
}
impl Broadcaster {
    pub fn new(
        sock: TcpStream,
        rx_data: tokio::sync::broadcast::Receiver<Bytes>,
        tx_control: tokio::sync::mpsc::Sender<ControlPacket>,
    ) -> Self {
        Self {
            sock,
            rx_data,
            tx_control,
        }
    }

    pub async fn run_server(self) {
        let mut sock = self.sock;
        let mut rx_data = self.rx_data;
        // let mut control_tx = self.tx_control;

        let mut tx_buf = BytesMut::with_capacity(16384);
        let mut rx_buf = BytesMut::with_capacity(16384);
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

        loop {
            tokio::select! {
                len = sock.read_buf(&mut rx_buf) => {
                    match len {
                        Err(_) | Ok(0) => {
                            break;
                        }
                        _ => {
                            if rx_buf.len() < 4 { continue; }

                            let packet_len = rx_buf.get_u32();
                            if rx_buf.len() < packet_len as usize { continue; }

                            let packet = rx_buf.split_to(packet_len as usize);
                            let data = Packer::<Packet>::unpack(packet.freeze());

                            if data.is_none() { continue; }

                            match data.unwrap() {
                                Packet::Control(control) => {
                                    self.tx_control.send(control).await.unwrap();
                                },
                                Packet::Handshake(version) => {
                                    tx_buf.extend_from_slice(&Packer::<Packet>::pack(Packet::HadnshakeResponse(version as u32)));

                                    println!("{:?}", tx_buf);
                                    sock.write_buf(&mut tx_buf).await;
                                }
                                _ => ()
                            }
                        }
                    }
                }
                data = rx_data.recv() => {
                    if data.is_err() {
                        break;
                    }

                    let data = data.unwrap();
                    tx_buf.extend_from_slice(&data);
                    sock.write_buf(&mut tx_buf).await;
                }
                _ = interval.tick(), if !tx_buf.is_empty() => {
                    sock.write_buf(&mut tx_buf).await;
                }
            }
        }
    }
}
