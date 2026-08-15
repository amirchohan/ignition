use super::*;
use std::{io::Write, net::TcpStream, path::Path, thread, time::Duration};

use crate::{
    domain::RecvTs,
    protocol::{MessageKind, SBE_HEADER_LEN, encode_header},
    runtime::Mode,
};

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ignition-{name}-{}-{}.bin", std::process::id(), unique_suffix()))
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos()
}

fn write_record(path: &Path, record: InputRecord) {
    let mut io = FileFrameIo::open(None, Some(path.to_path_buf()), 128).expect("writer");
    io.record_input(&record).expect("record");
    io.flush_pending().expect("flush");
}

fn frame(kind: MessageKind, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + payload.len()];
    encode_header(&mut bytes, payload.len() as u16, kind).expect("header");
    bytes[SBE_HEADER_LEN..].copy_from_slice(payload);
    bytes
}

#[test]
fn replay_file_backend_releases_only_expected_global_input_sequence() {
    let gateway_path = temp_path("gateway-replay");
    let control_path = temp_path("control-replay");
    write_record(
        &gateway_path,
        InputRecord {
            input_seq: 1,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(10),
            frame: vec![1, 2, 3],
        },
    );
    write_record(
        &control_path,
        InputRecord {
            input_seq: 2,
            endpoint: Endpoint::Control,
            recv_ts: RecvTs::new(11),
            frame: vec![4, 5, 6],
        },
    );

    let mut mode = Mode::Replay { expected_input_seq: 1 };
    let clock = Clock::new();
    let mut control = IORuntime::new(
        Endpoint::Control,
        IOConfig::File {
            input_path: Some(control_path.clone()),
            output_path: None,
            max_frame_len: 128,
        },
    )
    .expect("control");
    let mut gateway = IORuntime::new(
        Endpoint::Gateway,
        IOConfig::File {
            input_path: Some(gateway_path.clone()),
            output_path: None,
            max_frame_len: 128,
        },
    )
    .expect("gateway");

    let mut seen = Vec::new();
    {
        assert!(
            !control
                .poll(&clock, &mut mode, 128, |_, frame| {
                    seen.push((Endpoint::Control, frame.recv_ts.raw(), frame.bytes.to_vec()));
                    Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
                })
                .expect("control poll")
                .progress
        );
    }
    {
        assert!(
            gateway
                .poll(&clock, &mut mode, 128, |_, frame| {
                    seen.push((Endpoint::Gateway, frame.recv_ts.raw(), frame.bytes.to_vec()));
                    Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
                })
                .expect("gateway poll")
                .progress
        );
    }
    {
        assert!(
            control
                .poll(&clock, &mut mode, 128, |_, frame| {
                    seen.push((Endpoint::Control, frame.recv_ts.raw(), frame.bytes.to_vec()));
                    Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
                })
                .expect("control poll")
                .progress
        );
    }

    assert_eq!(
        seen,
        vec![(Endpoint::Gateway, 10, vec![1, 2, 3]), (Endpoint::Control, 11, vec![4, 5, 6])]
    );
    assert_eq!(mode, Mode::Replay { expected_input_seq: 3 });

    let _ = std::fs::remove_file(gateway_path);
    let _ = std::fs::remove_file(control_path);
}

#[test]
fn in_memory_backend_releases_replay_records_in_sequence() {
    let mut mode = Mode::Replay { expected_input_seq: 1 };
    let clock = Clock::new();
    let records = vec![
        InputRecord {
            input_seq: 1,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(10),
            frame: frame(MessageKind::NewOrder, &[1, 2, 3, 4]),
        },
        InputRecord {
            input_seq: 2,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(11),
            frame: frame(MessageKind::CancelOrder, &[5, 6, 7, 8]),
        },
    ];
    let mut io = IORuntime::new(
        Endpoint::Gateway,
        IOConfig::InMemory {
            input: records.clone(),
            max_frame_len: 128,
        },
    )
    .expect("in-memory io");
    let mut seen = Vec::new();

    assert!(
        io.poll(&clock, &mut mode, 1, |publisher, frame| {
            seen.push((frame.recv_ts.raw(), frame.bytes.to_vec()));
            publisher.queue_frame(frame.bytes)?;
            Ok::<FramePollControl, IoError>(FramePollControl::Continue)
        })
        .expect("first poll")
        .progress
    );
    assert!(
        io.poll(&clock, &mut mode, 1, |publisher, frame| {
            seen.push((frame.recv_ts.raw(), frame.bytes.to_vec()));
            publisher.queue_frame(frame.bytes)?;
            Ok::<FramePollControl, IoError>(FramePollControl::Continue)
        })
        .expect("second poll")
        .progress
    );
    assert!(
        !io.poll(&clock, &mut mode, 1, |_, _| {
            Ok::<FramePollControl, IoError>(FramePollControl::Continue)
        })
        .expect("drained")
        .progress
    );
    assert!(io.replay_input_exhausted().expect("exhausted"));
    assert_eq!(mode, Mode::Replay { expected_input_seq: 3 });
    assert_eq!(seen, vec![(10, records[0].frame.clone()), (11, records[1].frame.clone())]);
}

#[test]
fn canonical_poll_stops_after_accepting_the_triggering_frame() {
    let records = (1..=2)
        .map(|input_seq| InputRecord {
            input_seq,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(input_seq),
            frame: vec![input_seq as u8],
        })
        .collect();
    let mut io = IORuntime::new(
        Endpoint::Gateway,
        IOConfig::InMemory {
            input: records,
            max_frame_len: 128,
        },
    )
    .expect("in-memory io");
    let clock = Clock::new();
    let mut mode = Mode::Replay { expected_input_seq: 1 };
    let mut handled = 0;

    let status = io
        .poll(&clock, &mut mode, 2, |_, _| {
            handled += 1;
            Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Stop)
        })
        .expect("poll");

    assert_eq!(handled, 1);
    assert_eq!(
        status,
        FramePollStatus {
            progress: true,
            stopped: true,
        }
    );
    assert_eq!(mode, Mode::Replay { expected_input_seq: 2 });
    assert!(!io.replay_input_exhausted().expect("remaining input"));
}

#[test]
fn noop_backend_poll_is_idle() {
    let mut io = IORuntime::new(Endpoint::Control, IOConfig::Noop).expect("noop io");
    let clock = Clock::new();
    let mut mode = Mode::default();
    let mut handled = false;

    let status = io
        .poll(&clock, &mut mode, 1, |_, _| {
            handled = true;
            Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
        })
        .expect("poll");

    assert_eq!(status, FramePollStatus::IDLE);
    assert!(!handled);
}

#[test]
fn in_memory_backend_rejects_wrong_endpoint_record() {
    let mut mode = Mode::Replay { expected_input_seq: 1 };
    let clock = Clock::new();
    let mut io = IORuntime::new(
        Endpoint::Control,
        IOConfig::InMemory {
            input: vec![InputRecord {
                input_seq: 1,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(10),
                frame: vec![1, 2, 3],
            }],
            max_frame_len: 128,
        },
    )
    .expect("in-memory io");

    assert!(matches!(
        io.poll(&clock, &mut mode, 1, |_, _| {
            Ok::<FramePollControl, IoError>(FramePollControl::Continue)
        }),
        Err(FramePollError::Io(IoError::EndpointMismatch {
            configured: Endpoint::Control,
            record: Endpoint::Gateway,
        }))
    ));
}

#[test]
fn tcp_backend_records_live_input_frames() {
    let record_path = temp_path("tcp-recording");
    let clock = Clock::new();
    let mut runtime = match IORuntime::new(
        Endpoint::Control,
        IOConfig::Tcp {
            bind_addr: "127.0.0.1:0".to_owned(),
            max_frame_len: 128,
            max_pending_write_bytes: 1024,
            record_path: Some(record_path.clone()),
        },
    ) {
        Ok(runtime) => runtime,
        Err(IoError::Bind(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("tcp runtime: {error:?}"),
    };
    let addr = runtime.tcp_local_addr().expect("addr");
    let mut client = TcpStream::connect(addr).expect("connect");
    let frame = frame(MessageKind::Heartbeat, &[1, 0, 0, 0]);
    client.write_all(&frame).expect("write");

    let mut mode = Mode::default();
    let mut seen = Vec::new();
    for _ in 0..100 {
        runtime
            .poll(&clock, &mut mode, 128, |_, frame| {
                seen.push((frame.recv_ts, frame.bytes.to_vec()));
                Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
            })
            .expect("poll");
        if !seen.is_empty() {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    runtime.flush_pending().expect("flush");

    assert_eq!(seen.len(), 1);
    let mut recorded = FileFrameIo::open(Some(record_path.clone()), None, 128).expect("reader");
    let record = recorded.peek_record().expect("record").expect("record");
    assert_eq!(record.input_seq, 1);
    assert_eq!(record.endpoint, Endpoint::Control);
    assert_eq!(record.recv_ts, seen[0].0);
    assert_eq!(record.frame, frame);

    let _ = std::fs::remove_file(record_path);
}
