# jets

`jets` is a standalone messaging primitive built on:

- NATS subjects for low-latency routing
- JetStream for durability/replay/ack state
- Protobuf envelopes for strict typed contracts

It is intentionally just the transport + durable messaging core.
No tenant/user scoping, no product-specific auth, and no app business logic.

## Features

- Strict protobuf envelope contract (`proto/jets.proto`)
- Durable per-target inbox streams
- Per-source audit streams
- Global DLQ stream
- Message state tracking (`unread`, `acked`, `failed`, `expired`)
- Idempotency key guard rails
- DLQ replay

## Install

```bash
cargo add jets
```

## Environment

- `JETS_NATS_URL` (default: `nats://127.0.0.1:4222`)
- `JETS_SUBJECT_CMD_PREFIX` (default: `jets.cmd`)
- `JETS_SUBJECT_EVT_PREFIX` (default: `jets.evt`)
- `JETS_SUBJECT_DLQ` (default: `jets.dlq`)
- `JETS_INBOX_STREAM_PREFIX` (default: `INBOX`)
- `JETS_AUDIT_STREAM_PREFIX` (default: `AUDIT`)
- `JETS_DLQ_STREAM` (default: `JETS_DLQ`)
- `JETS_STATE_KV` (default: `JETS_MSG_STATE`)
- `JETS_IDEMPOTENCY_KV` (default: `JETS_IDEMPOTENCY`)
- `JETS_DATA_KV` (default: `JETS_MSG_DATA`)

## Quick Example

```rust
use jets::{JetsService, PublishInput};

# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
let jets = JetsService::from_env();
let envelope = jets::proto::Envelope {
    version: 1,
    msg_id: "msg-1".into(),
    request_id: "req-1".into(),
    trace_id: "trace-1".into(),
    correlation_id: "corr-1".into(),
    causation_id: "".into(),
    from: "source-a".into(),
    to: "target-b".into(),
    stream_key: "target-b".into(),
    seq: 0,
    sent_at: chrono::Utc::now().timestamp_millis() as u64,
    ttl_ms: 30_000,
    permission_level: jets::proto::PermissionLevel::Full as i32,
    idempotency_key: "idmp-1".into(),
    payload: Some(jets::proto::Payload {
        kind: Some(jets::proto::payload::Kind::LifecycleAction(
            jets::proto::LifecycleAction {
                action: "restart".into(),
                desired_state_version: 1,
                apply_mode: jets::proto::ApplyMode::Enforce as i32,
            },
        )),
    }),
};

let _published = jets.publish(PublishInput { envelope }).await?;
# Ok(())
# }
```

## Development

```bash
cargo check
cargo test
```
