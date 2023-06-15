use broadcaster::BroadcasterMangement;
use futures_util::sink::SinkExt;

use std::io::{Read};

use std::process::Stdio;

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt},
    process::ChildStdout,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};


mod broadcaster;
mod packer;
// mod client;
// mod room;
// mod stream;

enum FFMpegEvent {
    Header(Bytes),
    Data(Bytes),
    NoOP,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build();

    if let Ok(rt) = runtime {
        rt.block_on(ffmpeg_stream())
    };
}

async fn broadcast_worker(mut rx: UnboundedReceiver<FFMpegEvent>) {
    let mut header = Bytes::new();

    let mut buf = BytesMut::with_capacity(524_288);
    let (broadcast_tx, broadcast_rx) = tokio::sync::broadcast::channel::<Bytes>(30);

    let broadcasteres = BroadcasterMangement::new(broadcast_rx);
    tokio::spawn(broadcasteres.run_server(8080));

    while let Some(msg) = rx.recv().await {
        match msg {
            FFMpegEvent::Header(data) => header = data,
            FFMpegEvent::Data(data) => {
                buf.reserve(data.len() + header.len());
                buf.put(header.clone());
                buf.put(data);

                broadcast_tx.send(buf.split().freeze());

                buf.clear();
            }
            _ => {}
        }
    }
}
async fn ffmpeg_stream() {
    // ffmpeg -v verbose -listen 1 -i rtmp://0.0.0.0:44444 -c copy -f mp4 -g 25 -movflags frag_keyframe+empty_moov pipe:1

    let (mut tx, rx) = tokio::sync::mpsc::unbounded_channel::<FFMpegEvent>();
    tokio::spawn(broadcast_worker(rx));

    let ffmpeg = tokio::process::Command::new("ffmpeg")
        .arg("-v")
        .arg("verbose")
        .arg("-listen")
        .arg("1")
        .arg("-i")
        .arg("rtmp://127.0.0.1:44445/live")
        // .arg("-c:v")
        .arg("-c")
        .arg("copy")
        // .arg("-c:v")
        // .arg("libx264")
        // .arg("-x264opts")
        // .arg("keyint=30:min-keyint=30")
        .arg("-keyint_min")
        .arg("30")
        .arg("-force_key_frames")
        .arg("expr:gte(t,n_forced*1)")
        // .arg("-c:a")
        // .arg("copy")
        // .arg("-acodec")
        // .arg("aac")
        .arg("-f")
        .arg("mp4")
        .arg("-g")
        .arg("30")
        .arg("-movflags")
        .arg("frag_keyframe+empty_moov+default_base_moof")
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .spawn();

    // ffmpeg -listen 1 -i rtmp://127.0.0.1:44445/live -f x11grab -framerate 30 -i :0.0 \
    // -vcodec libx264 -x264-params keyint=1 -t 5 -pix_fmt yuv420p \
    // -f ssegment -segment_time 0.001 -segment_format mp4 \
    // -segment_format_options movflags=+frag_keyframe+empty_moov+default_base_moof pipe:1

    // let mut ffmpeg = tokio::process::Command::new("ffmpeg")
    //     .arg("-v")
    //     .arg("verbose")
    //     .arg("-listen")
    //     .arg("1")
    //     .arg("-i")
    //     .arg("rtmp://127.0.0.1:44445/live")
    //     .arg("-vcodec")
    //     .arg("libx264")
    //     .arg("-x264-params")
    //     .arg("keyint=1")
    //     .arg("-t")
    //     .arg("5")
    //     .arg("-f")
    //     .arg("mp4")
    // .arg("-f")
    // .arg("ssegment")
    // .arg("-segment_time")
    // .arg("0.001")
    // .arg("-segment_format")
    // .arg("mp4")
    // .arg("-segment_format_options")
    // .arg("movieflags+=frag_keyframe+empty_moov+default_base_moof")
    // .arg("-movflags")
    // .arg("frag_keyframe+empty_moov+default_base_moof")
    // .arg("pipe:1")
    // .stdout(std::process::Stdio::piped())
    // .spawn();

    if let Ok(mut ffmpeg) = ffmpeg {
        let mut stdout = ffmpeg.stdout.take().unwrap();
        let processer = MP4Processer::new(Mp4HeaderProcesser);

        let mut data_processer = processer.process(&mut stdout, &mut tx).await.unwrap();
        while let Ok(()) = data_processer.process(&mut stdout, &mut tx).await {}
    }
}

trait Processer {}
struct MP4Processer<T>
where
    T: Processer,
{
    header_len: usize,
    buffer: BytesMut,
    state: T,
}
impl<T> MP4Processer<T>
where
    T: Processer,
{
    fn new(state: T) -> Self {
        MP4Processer {
            buffer: BytesMut::new(),
            header_len: 0,
            state,
        }
    }
}
impl MP4Processer<Mp4HeaderProcesser> {
    async fn process(
        mut self,
        reader: &mut ChildStdout,
        tx: &mut UnboundedSender<FFMpegEvent>,
    ) -> Result<MP4Processer<Mp4DataProcesser>, &'static str> {
        let event = self.state.process(reader, &mut self.buffer).await?;
        tx.send(event.0);
        Ok(MP4Processer {
            header_len: event.1,
            buffer: BytesMut::new(),
            state: Mp4DataProcesser(Mp4DataProcessState::MoofLen),
        })

        // if let Ok(event) = self.state.process(reader, &mut self.buffer).await {
        //     tx.send(event);
        //     return Ok(MP4Processer {
        //         buffer: BytesMut::new(),
        //         state: Mp4DataProcesser(Mp4DataProcessState::MoofLen),
        //     });
        // }
        // return Err(());
    }
}
impl MP4Processer<Mp4DataProcesser> {
    async fn process(
        &mut self,
        reader: &mut ChildStdout,
        tx: &mut UnboundedSender<FFMpegEvent>,
    ) -> Result<(), ()> {
        if let Ok(event) = self
            .state
            .process(reader, &mut self.buffer, self.header_len)
            .await
        {
            match event {
                FFMpegEvent::NoOP => {}
                _ => {
                    tx.send(event);
                }
            }
            return Ok(());
        }

        Err(())
    }
}

struct Mp4HeaderProcesser;
impl Processer for Mp4HeaderProcesser {}
struct Mp4DataProcesser(Mp4DataProcessState);
impl Processer for Mp4DataProcesser {}

impl Mp4HeaderProcesser {
    async fn process(
        &mut self,
        reader: &mut ChildStdout,
        _buffer: &mut BytesMut,
    ) -> Result<(FFMpegEvent, usize), &'static str> {
        let mut header = BytesMut::with_capacity(2048);
        let mut buf_temp = BytesMut::new();

        let ftype_len = reader.read_u32().await.unwrap();
        if ftype_len > 8192 {
            return Err("ftype too long");
        }

        header.put_u32(ftype_len);
        buf_temp.resize((ftype_len - 4) as usize, 0);
        let _ftype_box = reader.read_exact(&mut buf_temp).await.unwrap();
        header.extend_from_slice(&buf_temp);

        let moov_len = reader.read_u32().await.unwrap();
        if moov_len > 8192 {
            return Err("moov too long");
        }
        header.put_u32(moov_len);
        buf_temp.resize((moov_len - 4) as usize, 0);
        let _moov_box = reader.read_exact(&mut buf_temp).await.unwrap();
        header.put(buf_temp);

        let len = header.len();
        Ok((FFMpegEvent::Header(header.freeze()), len))
    }
}
enum Mp4DataProcessState {
    MoofLen,
    Moof(usize),         // len
    MdataLen(Bytes),     // moof data
    Mdata(usize, Bytes), // len + moof data
}
impl Mp4DataProcesser {
    async fn process(
        &mut self,
        reader: &mut ChildStdout,
        buffer: &mut BytesMut,
        header_len: usize,
    ) -> Result<FFMpegEvent, &'static str> {
        let len = reader
            .read_buf(buffer)
            .await
            .map_err(|_| "error while read")?;
        if len == 0 {
            return Err("ffmpeg closed");
        }

        if let Mp4DataProcessState::MoofLen = self.0 {
            // println!("phase MoofLen");

            if buffer.len() < 4 {
                return Ok(FFMpegEvent::NoOP);
            }

            let moof_len = buffer.get_u32();
            self.0 = Mp4DataProcessState::Moof((moof_len - 4) as usize);
        }
        if let Mp4DataProcessState::Moof(ref mut len) = self.0 {
            // println!("phase Moof");

            if buffer.len() < *len {
                return Ok(FFMpegEvent::NoOP);
            }

            let mut moof_data = buffer.split_to(*len);
            Self::change_tfhd_base_data_offset(&mut moof_data, header_len);
            self.0 = Mp4DataProcessState::MdataLen(moof_data.freeze());
        }
        if let Mp4DataProcessState::MdataLen(ref moof_data) = self.0 {
            // println!("phase MdataLen");

            if buffer.len() < 4 {
                return Ok(FFMpegEvent::NoOP);
            }

            let mdat_len = buffer.get_u32();
            self.0 = Mp4DataProcessState::Mdata((mdat_len - 4) as usize, moof_data.clone());
        }
        if let Mp4DataProcessState::Mdata(len, ref moof_data) = self.0 {
            // println!("phase Mdata");

            if buffer.len() < len {
                return Ok(FFMpegEvent::NoOP);
            }

            let mdat_data = buffer.split_to(len);
            let mut data = BytesMut::with_capacity(4 + moof_data.len() + 4 + mdat_data.len());
            data.put_u32((moof_data.len() + 4) as u32);
            data.extend_from_slice(moof_data);
            data.put_u32((mdat_data.len() + 4) as u32);
            data.put(mdat_data);

            self.0 = Mp4DataProcessState::MoofLen;

            return Ok(FFMpegEvent::Data(data.freeze()));
        }

        Ok(FFMpegEvent::NoOP)
    }

    fn change_tfhd_base_data_offset(buf: &mut BytesMut, _header_len: usize) {
        let mut ptr: &mut [u8] = &mut buf[4..];

        while ptr.len() > 8 {
            let box_len = BigEndian::read_u32(ptr);
            let box_type = &ptr[4..8];

            // println!(
            //     "box len: {}, box type: {:?}",
            //     box_len,
            //     std::str::from_utf8(box_type)
            // );
            match box_type {
                b"traf" => {
                    // moof -> traf -> tfhd
                    // 그러므로, traf면 다시 안으로 이동 해야함.
                    // 하지만... 그걸 다 구현하면 힘드니까 그냥 스킵하는 방식으로 구현

                    ptr = &mut ptr[8..];
                }
                // b"tfhd" => {
                //     // set Default Base-Data-Offset flag to 0
                //     let mut flags = BigEndian::read_u32(&ptr[8..12]);
                //     // println!("tfhd found, flags: {} -> {}", flags, flags & 0xfffffffe);
                //     flags &= 0xfffffffe;

                //     BigEndian::write_u32(&mut ptr[8..12], flags);

                //     // set Default Base-Data-Offset to 0
                //     BigEndian::write_u64(&mut ptr[16..24], header_len as u64);
                //     // BigEndian::write_u64(&mut ptr[16..24], 0);

                //     ptr = &mut ptr[box_len as usize..];
                // }
                // b"mfhd" => {
                //     // println!("mfhd found, seq = {}", BigEndian::read_u32(&ptr[12..16]));
                //     // set sequence number to always 1
                //     BigEndian::write_u32(&mut ptr[12..16], 1);

                //     ptr = &mut ptr[box_len as usize..];
                // }
                // b"tfdt" => {
                //     // println!(
                //     //     "tfdt found, baseMediaDecodeTime = {}",
                //     //     BigEndian::read_u64(&ptr[12..20])
                //     // );
                //     // set base media decode time to 0

                //     BigEndian::write_u64(&mut ptr[12..20], 0);
                //     ptr = &mut ptr[box_len as usize..];
                // }
                _ => {
                    ptr = &mut ptr[box_len as usize..];
                }
            }
        }
    }
}

// #[derive(Debug)]
// pub struct Mp4Box {
//     pub size: u32,
//     pub box_type: String,
// }

// impl Mp4Box {
//     pub fn from_reader(reader: &mut BufReader<File>) -> Option<Self> {
//         let mut size_buf = [0; 4];
//         let mut box_type_buf = [0; 4];

//         if reader.read_exact(&mut size_buf).is_err() {
//             return None;
//         }

//         let size = u32::from_be_bytes(size_buf);
//         if size < 8 {
//             return None;
//         }

//         if reader.read_exact(&mut box_type_buf).is_err() {
//             return None;
//         }

//         let box_type = String::from_utf8_lossy(&box_type_buf).to_string();
//         let box_content_size = size - 8;
//         reader
//             .seek(SeekFrom::Current(box_content_size as i64))
//             .unwrap();

//         Some(Mp4Box { size, box_type })
//     }
// }

// pub fn split_mp4(input_file_path: &Path, output_file_path: &str, header: Vec<u8>) {
//     let input_file = File::open(input_file_path).expect("Unable to open MP4 file");
//     let mut reader = BufReader::new(input_file);
//     let mut file_counter = 0;

//     let mut last_pos = 0;
//     while let Some(box_data) = Mp4Box::from_reader(&mut reader) {
//         println!("-- {:?} ", box_data.box_type);

//         if box_data.box_type == "mdat" {
//             let mut output_file =
//                 File::create(format!("{}_{}.mp4", output_file_path, file_counter))
//                     .expect("Unable to create output file");

//             let end_pos = reader.stream_position().unwrap();
//             println!("current pos: {:?}", end_pos);
//             // let start_pos = end_pos - box_data.size as u64;

//             reader.seek(SeekFrom::Start(last_pos)).unwrap();

//             let mut buffer = vec![0; (end_pos - last_pos) as usize];
//             reader.read_exact(&mut buffer).unwrap();
//             output_file.write_all(&header).unwrap();
//             output_file.write_all(&buffer).unwrap();

//             file_counter += 1;

//             reader.seek(SeekFrom::Start(end_pos)).unwrap();
//             last_pos = end_pos;
//         }
//     }
// }

// fn main() {
//     let hexstr = "000000246674797069736F6D0000020069736F6D69736F326176633169736F366D703431000004BD6D6F6F760000006C6D766864000000000000000000000000000003E8000000000001000001000000000000000000000000010000000000000000000000000000000100000000000000000000000000004000000000000000000000000000000000000000000000000000000000000002000001F47472616B0000005C746B6864000000030000000000000000000000010000000000000000000000000000000000000000000000000001000000000000000000000000000000010000000000000000000000000000400000000780000004380000000001906D646961000000206D64686400000000000000000000000000003E800000000055C400000000002D68646C72000000000000000076696465000000000000000000000000566964656F48616E646C6572000000013B6D696E6600000014766D68640000000100000000000000000000002464696E660000001C6472656600000000000000010000000C75726C2000000001000000FB7374626C000000AF7374736400000000000000010000009F6176633100000000000000010000000000000000000000000000000007800438004800000048000000000000000100000000000000000000000000000000000000000000000000000000000000000018FFFF000000396176634301640429FFE1002267640429ACB403C0113F2FFE0002000284000009000003000100000300328F08846A01000468EE3C80000000107061737000000001000000010000001073747473000000000000000000000010737473630000000000000000000000147374737A000000000000000000000000000000107374636F0000000000000000000001AB7472616B0000005C746B6864000000030000000000000000000000020000000000000000000000000000000000000001010000000001000000000000000000000000000000010000000000000000000000000000400000000000000000000000000001476D646961000000206D6468640000000000000000000000000000BB800000000055C400000000002D68646C720000000000000000736F756E000000000000000000000000536F756E6448616E646C657200000000F26D696E6600000010736D686400000000000000000000002464696E660000001C6472656600000000000000010000000C75726C2000000001000000B67374626C0000006A7374736400000000000000010000005A6D703461000000000000000100000000000000000002001000000000BB80000000000036657364730000000003808080250002000480808017401500000000030000000000000580808005119056E5000680808001020000001073747473000000000000000000000010737473630000000000000000000000147374737A000000000000000000000000000000107374636F0000000000000000000000486D7665780000002074726578000000000000000100000001000000000000000000000000000000207472657800000000000000020000000100000000000000000000000000000062756474610000005A6D657461000000000000002168646C7200000000000000006D6469726170706C0000000000000000000000002D696C737400000025A9746F6F0000001D6461746100000001000000004C61766635382E32392E313030000002A06D6F6F66000000106D6668640000000000000001000000BC747261660000002474666864000000390000000100000000000004E1000002800001F0660101000000000014746664740100000000000000000000000000007C7472756E0000020500000019000002A8020000000001F06600001A5B0000039D0000025600000B6500000F74000009C80000052A00002D2100004462000044620000446200004462000044620000446200004462000044620000446200004462000044620000446200004462000044620000446200004462000001CC747261660000002474666864000000390000000200000000000004E100000400000001DC0200000000000014746664740100000000000000000000000000018C7472756E000003010000002F0006B06800000400000001DC000003F80000017000000400000001C400000400000001D3000004000000021F000004000000028E0000040000000277000004000000024F00000400000001FB000004000000022D00000400000001C500000400000001A0000004000000017400000400000001DC00000400000001E70000040000000263000004000000027B00000400000001FF00000400000001A800000400000001C300000400000001C200000400000001ED000004000000024A000004000000029C000004000000027700000400000002430000040000000216000004000000019A000004000000018B00000400000001A900000400000001C900000400000001C900000400000001F600000400000002020000040000000202000004000000024A00000400000002310000040000000220000004000000029D00000400000001FB00000400000001FF000004000000023F000004000000024C000004000000022F000004000000020600000400000002470000040000000227";
//     // let file_path = Path::new("test.mp4");
//     let file_path = Path::new("test.mp4");
//     let output_file_path = "output/split_file";

//     split_mp4(&file_path, output_file_path, hex::decode(hexstr).unwrap());
// }
