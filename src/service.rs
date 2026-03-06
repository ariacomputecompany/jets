use crate::proto::{self as jets_proto, Envelope, PermissionLevel};
use async_nats::jetstream;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use prost::Message;
use std::collections::HashMap;

const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const DEFAULT_SUBJECT_CMD_PREFIX: &str = "jets.cmd";
const DEFAULT_SUBJECT_EVT_PREFIX: &str = "jets.evt";
const DEFAULT_SUBJECT_DLQ: &str = "jets.dlq";
const DEFAULT_INBOX_STREAM_PREFIX: &str = "INBOX";
const DEFAULT_AUDIT_STREAM_PREFIX: &str = "AUDIT";
const DEFAULT_DLQ_STREAM: &str = "JETS_DLQ";
const DEFAULT_STATE_KV: &str = "JETS_MSG_STATE";
const DEFAULT_IDEMPOTENCY_KV: &str = "JETS_IDEMPOTENCY";
const DEFAULT_DATA_KV: &str = "JETS_MSG_DATA";

#[derive(Debug)]
pub enum JetsError {
    InvalidInput(String),
    Transport(String),
    Conflict(String),
    NotFound(String),
    Internal(String),
}

impl std::fmt::Display for JetsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JetsError::InvalidInput(v)
            | JetsError::Transport(v)
            | JetsError::Conflict(v)
            | JetsError::NotFound(v)
            | JetsError::Internal(v) => write!(f, "{}", v),
        }
    }
}
impl std::error::Error for JetsError {}

#[derive(Clone)]
pub struct JetsService {
    nats_url: String,
    subject_cmd_prefix: String,
    subject_evt_prefix: String,
    subject_dlq: String,
    inbox_stream_prefix: String,
    audit_stream_prefix: String,
    dlq_stream: String,
    state_kv: String,
    idempotency_kv: String,
    data_kv: String,
}

#[derive(Debug, Clone)]
pub struct PublishInput {
    pub envelope: Envelope,
}

#[derive(Debug, Clone)]
pub struct PublishResult {
    pub stream: String,
    pub subject: String,
    pub stream_seq: u64,
    pub duplicate: bool,
}

#[derive(Debug, Clone)]
pub struct ReadInboxFilter {
    pub target: String,
    pub from_seq: Option<u64>,
    pub to_seq: Option<u64>,
    pub start_time_ms: Option<u64>,
    pub end_time_ms: Option<u64>,
    pub state: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct InboxMessage {
    pub stream: String,
    pub stream_seq: u64,
    pub state: String,
    pub envelope_b64: String,
    pub envelope: Envelope,
}

#[derive(Debug, Clone)]
pub enum AckAction {
    Ack,
    Nack,
}

#[derive(Debug, Clone)]
pub struct AckResult {
    pub msg_id: String,
    pub new_state: String,
    pub dlq_forwarded: bool,
}

#[derive(Debug, Clone)]
pub struct ListDlqFilter {
    pub from_seq: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Clone)]
pub struct DlqMessage {
    pub stream_seq: u64,
    pub envelope_b64: String,
    pub envelope: Envelope,
}

impl Default for JetsService {
    fn default() -> Self {
        Self::from_env()
    }
}

impl JetsService {
    pub fn nats_url(&self) -> &str {
        &self.nats_url
    }

    pub fn from_env() -> Self {
        Self {
            nats_url: std::env::var("JETS_NATS_URL")
                .unwrap_or_else(|_| DEFAULT_NATS_URL.to_string()),
            subject_cmd_prefix: std::env::var("JETS_SUBJECT_CMD_PREFIX")
                .unwrap_or_else(|_| DEFAULT_SUBJECT_CMD_PREFIX.to_string()),
            subject_evt_prefix: std::env::var("JETS_SUBJECT_EVT_PREFIX")
                .unwrap_or_else(|_| DEFAULT_SUBJECT_EVT_PREFIX.to_string()),
            subject_dlq: std::env::var("JETS_SUBJECT_DLQ")
                .unwrap_or_else(|_| DEFAULT_SUBJECT_DLQ.to_string()),
            inbox_stream_prefix: std::env::var("JETS_INBOX_STREAM_PREFIX")
                .unwrap_or_else(|_| DEFAULT_INBOX_STREAM_PREFIX.to_string()),
            audit_stream_prefix: std::env::var("JETS_AUDIT_STREAM_PREFIX")
                .unwrap_or_else(|_| DEFAULT_AUDIT_STREAM_PREFIX.to_string()),
            dlq_stream: std::env::var("JETS_DLQ_STREAM")
                .unwrap_or_else(|_| DEFAULT_DLQ_STREAM.to_string()),
            state_kv: std::env::var("JETS_STATE_KV")
                .unwrap_or_else(|_| DEFAULT_STATE_KV.to_string()),
            idempotency_kv: std::env::var("JETS_IDEMPOTENCY_KV")
                .unwrap_or_else(|_| DEFAULT_IDEMPOTENCY_KV.to_string()),
            data_kv: std::env::var("JETS_DATA_KV").unwrap_or_else(|_| DEFAULT_DATA_KV.to_string()),
        }
    }

    async fn js(&self) -> Result<jetstream::Context, JetsError> {
        let client = async_nats::connect(self.nats_url.clone())
            .await
            .map_err(|e| JetsError::Transport(format!("failed to connect to NATS: {}", e)))?;
        Ok(jetstream::new(client))
    }

    pub async fn health_check(&self) -> Result<(), JetsError> {
        let _ = self.js().await?;
        Ok(())
    }

    fn stream_name(prefix: &str, target: &str) -> String {
        let mut out = String::new();
        for c in target.chars() {
            if c.is_ascii_alphanumeric() {
                out.push(c.to_ascii_uppercase());
            } else {
                out.push('_');
            }
        }
        format!("{}_{}", prefix, out)
    }

    fn subject_for_cmd(&self, target: &str) -> String {
        format!("{}.{}", self.subject_cmd_prefix, target)
    }

    fn subject_for_evt(&self, source: &str) -> String {
        format!("{}.{}", self.subject_evt_prefix, source)
    }

    fn validate_envelope(&self, env: &Envelope) -> Result<(), JetsError> {
        if env.version == 0 {
            return Err(JetsError::InvalidInput("version must be > 0".to_string()));
        }
        if env.msg_id.trim().is_empty() || env.from.trim().is_empty() || env.to.trim().is_empty() {
            return Err(JetsError::InvalidInput(
                "msg_id, from, and to are required".to_string(),
            ));
        }
        if env.ttl_ms == 0 || env.sent_at == 0 {
            return Err(JetsError::InvalidInput(
                "ttl_ms and sent_at are required".to_string(),
            ));
        }
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        if now_ms > env.sent_at.saturating_add(env.ttl_ms) {
            return Err(JetsError::InvalidInput(
                "message is expired by ttl".to_string(),
            ));
        }
        let payload = env
            .payload
            .as_ref()
            .ok_or_else(|| JetsError::InvalidInput("payload is required".to_string()))?;
        if payload.kind.is_none() {
            return Err(JetsError::InvalidInput(
                "payload kind is required".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_permission(&self, env: &Envelope) -> Result<(), JetsError> {
        let level = PermissionLevel::try_from(env.permission_level)
            .map_err(|_| JetsError::InvalidInput("invalid permission_level".to_string()))?;
        let payload_kind = env
            .payload
            .as_ref()
            .and_then(|p| p.kind.as_ref())
            .ok_or_else(|| JetsError::InvalidInput("payload kind is required".to_string()))?;

        let allowed = match payload_kind {
            jets_proto::payload::Kind::StateSnapshot(_) => {
                matches!(
                    level,
                    PermissionLevel::ReadOnly
                        | PermissionLevel::WriteState
                        | PermissionLevel::Admin
                        | PermissionLevel::Full
                )
            }
            jets_proto::payload::Kind::StatePatch(_)
            | jets_proto::payload::Kind::FilePatch(_)
            | jets_proto::payload::Kind::EnvPatch(_)
            | jets_proto::payload::Kind::LifecycleAction(_) => {
                matches!(
                    level,
                    PermissionLevel::WriteState | PermissionLevel::Admin | PermissionLevel::Full
                )
            }
            jets_proto::payload::Kind::ExecCommand(_) => {
                matches!(
                    level,
                    PermissionLevel::Execute | PermissionLevel::Admin | PermissionLevel::Full
                )
            }
            jets_proto::payload::Kind::Ack(_) | jets_proto::payload::Kind::Error(_) => {
                matches!(level, PermissionLevel::Admin | PermissionLevel::Full)
            }
        };
        if !allowed {
            return Err(JetsError::InvalidInput(
                "permission level does not allow payload kind".to_string(),
            ));
        }
        Ok(())
    }

    async fn ensure_stream(
        &self,
        js: &jetstream::Context,
        stream_name: String,
        subject: String,
    ) -> Result<(), JetsError> {
        let cfg = jetstream::stream::Config {
            name: stream_name,
            subjects: vec![subject],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_age: std::time::Duration::from_secs(0),
            max_messages: -1,
            max_bytes: -1,
            duplicate_window: std::time::Duration::from_secs(120),
            allow_rollup: false,
            deny_delete: true,
            deny_purge: true,
            ..Default::default()
        };
        js.get_or_create_stream(cfg)
            .await
            .map_err(|e| JetsError::Transport(format!("failed to ensure stream: {}", e)))?;
        Ok(())
    }

    async fn ensure_dlq_stream(&self, js: &jetstream::Context) -> Result<(), JetsError> {
        self.ensure_stream(js, self.dlq_stream.clone(), self.subject_dlq.clone())
            .await
    }

    async fn ensure_kv(
        &self,
        js: &jetstream::Context,
        bucket: &str,
        history: i64,
        max_age_secs: u64,
    ) -> Result<jetstream::kv::Store, JetsError> {
        match js.get_key_value(bucket.to_string()).await {
            Ok(store) => Ok(store),
            Err(_) => {
                js.create_key_value(jetstream::kv::Config {
                    bucket: bucket.to_string(),
                    history,
                    max_age: std::time::Duration::from_secs(max_age_secs),
                    ..Default::default()
                })
                .await
                .map_err(|e| JetsError::Transport(format!("failed to ensure kv {}: {}", bucket, e)))
            }
        }
    }

    async fn set_msg_state(
        &self,
        kv: &jetstream::kv::Store,
        msg_id: &str,
        state: &str,
        stream_seq: u64,
    ) -> Result<(), JetsError> {
        let payload = serde_json::json!({
            "state": state,
            "stream_seq": stream_seq,
            "updated_at_ms": chrono::Utc::now().timestamp_millis() as u64,
        })
        .to_string();

        kv.put(msg_id, payload.into())
            .await
            .map_err(|e| JetsError::Internal(format!("failed to write state: {}", e)))?;
        Ok(())
    }

    async fn get_msg_state(
        &self,
        kv: &jetstream::kv::Store,
        msg_id: &str,
    ) -> Result<Option<String>, JetsError> {
        let Some(entry) = kv
            .get(msg_id)
            .await
            .map_err(|e| JetsError::Internal(format!("failed to read state: {}", e)))?
        else {
            return Ok(None);
        };
        let parsed: serde_json::Value = serde_json::from_slice(&entry)
            .map_err(|e| JetsError::Internal(format!("invalid state json: {}", e)))?;
        Ok(Some(
            parsed
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or("unread")
                .to_string(),
        ))
    }

    pub async fn publish(&self, input: PublishInput) -> Result<PublishResult, JetsError> {
        self.validate_envelope(&input.envelope)?;
        self.validate_permission(&input.envelope)?;

        let js = self.js().await?;
        let inbox_stream = Self::stream_name(&self.inbox_stream_prefix, &input.envelope.to);
        let audit_stream = Self::stream_name(&self.audit_stream_prefix, &input.envelope.from);
        let inbox_subject = self.subject_for_cmd(&input.envelope.to);
        let audit_subject = self.subject_for_evt(&input.envelope.from);

        self.ensure_stream(&js, inbox_stream.clone(), inbox_subject.clone())
            .await?;
        self.ensure_stream(&js, audit_stream, audit_subject.clone())
            .await?;
        self.ensure_dlq_stream(&js).await?;

        let state_kv = self.ensure_kv(&js, &self.state_kv, 5, 0).await?;
        let idem_kv = self
            .ensure_kv(&js, &self.idempotency_kv, 1, 24 * 3600)
            .await?;
        let data_kv = self.ensure_kv(&js, &self.data_kv, 3, 7 * 24 * 3600).await?;

        if !input.envelope.idempotency_key.is_empty() {
            let idem_key = format!("{}:{}", input.envelope.to, input.envelope.idempotency_key);
            if idem_kv
                .get(&idem_key)
                .await
                .map_err(|e| JetsError::Internal(format!("failed to read idempotency kv: {}", e)))?
                .is_some()
            {
                return Err(JetsError::Conflict(
                    "idempotency key already used".to_string(),
                ));
            }
        }

        let bytes = Message::encode_to_vec(&input.envelope);
        let ack = js
            .publish(inbox_subject.clone(), bytes.clone().into())
            .await
            .map_err(|e| JetsError::Transport(format!("failed to publish: {}", e)))?
            .await
            .map_err(|e| JetsError::Transport(format!("failed to ack publish: {}", e)))?;

        self.set_msg_state(&state_kv, &input.envelope.msg_id, "unread", ack.sequence)
            .await?;
        data_kv
            .put(&input.envelope.msg_id, bytes.clone().into())
            .await
            .map_err(|e| JetsError::Internal(format!("failed to store message bytes: {}", e)))?;

        if !input.envelope.idempotency_key.is_empty() {
            let idem_key = format!("{}:{}", input.envelope.to, input.envelope.idempotency_key);
            idem_kv
                .put(
                    &idem_key,
                    serde_json::json!({
                        "msg_id": input.envelope.msg_id,
                        "stream": inbox_stream,
                        "seq": ack.sequence,
                    })
                    .to_string()
                    .into(),
                )
                .await
                .map_err(|e| {
                    JetsError::Internal(format!("failed to write idempotency marker: {}", e))
                })?;
        }

        js.publish(audit_subject, bytes.into())
            .await
            .map_err(|e| JetsError::Transport(format!("failed to publish audit event: {}", e)))?
            .await
            .map_err(|e| JetsError::Transport(format!("failed to ack audit event: {}", e)))?;

        Ok(PublishResult {
            stream: inbox_stream,
            subject: inbox_subject,
            stream_seq: ack.sequence,
            duplicate: ack.duplicate,
        })
    }

    pub async fn read_inbox(
        &self,
        filter: ReadInboxFilter,
    ) -> Result<Vec<InboxMessage>, JetsError> {
        if filter.target.trim().is_empty() {
            return Err(JetsError::InvalidInput("target is required".to_string()));
        }

        let js = self.js().await?;
        let stream_name = Self::stream_name(&self.inbox_stream_prefix, &filter.target);
        self.ensure_stream(
            &js,
            stream_name.clone(),
            self.subject_for_cmd(&filter.target),
        )
        .await?;
        let state_kv = self.ensure_kv(&js, &self.state_kv, 5, 0).await?;
        let mut stream = js
            .get_stream(stream_name.clone())
            .await
            .map_err(|e| JetsError::Transport(format!("failed to get inbox stream: {}", e)))?;
        let info = stream
            .info()
            .await
            .map_err(|e| JetsError::Transport(format!("failed to get stream info: {}", e)))?;

        let mut current = filter.from_seq.unwrap_or(info.state.first_sequence);
        let end = filter.to_seq.unwrap_or(info.state.last_sequence);
        if current == 0 || end == 0 || current > end {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        while current <= end && out.len() < filter.limit {
            let raw = match stream.get_raw_message(current).await {
                Ok(v) => v,
                Err(_) => {
                    current += 1;
                    continue;
                }
            };

            let envelope = Envelope::decode(raw.payload.as_ref())
                .map_err(|e| JetsError::Internal(format!("failed to decode envelope: {}", e)))?;

            if let Some(start_ms) = filter.start_time_ms {
                if envelope.sent_at < start_ms {
                    current += 1;
                    continue;
                }
            }
            if let Some(end_ms) = filter.end_time_ms {
                if envelope.sent_at > end_ms {
                    current += 1;
                    continue;
                }
            }

            let state = self
                .get_msg_state(&state_kv, &envelope.msg_id)
                .await?
                .unwrap_or_else(|| "unread".to_string());

            if let Some(expected) = filter.state.as_ref() {
                if state != *expected {
                    current += 1;
                    continue;
                }
            }

            out.push(InboxMessage {
                stream: stream_name.clone(),
                stream_seq: current,
                state,
                envelope_b64: BASE64.encode(raw.payload.as_ref()),
                envelope,
            });
            current += 1;
        }

        Ok(out)
    }

    pub async fn ack_message(
        &self,
        msg_id: &str,
        action: AckAction,
        reason: Option<&str>,
    ) -> Result<AckResult, JetsError> {
        if msg_id.trim().is_empty() {
            return Err(JetsError::InvalidInput("msg_id is required".to_string()));
        }

        let js = self.js().await?;
        self.ensure_dlq_stream(&js).await?;
        let state_kv = self.ensure_kv(&js, &self.state_kv, 5, 0).await?;
        let data_kv = self.ensure_kv(&js, &self.data_kv, 3, 7 * 24 * 3600).await?;

        let new_state = match action {
            AckAction::Ack => "acked",
            AckAction::Nack => "failed",
        };
        self.set_msg_state(&state_kv, msg_id, new_state, 0).await?;

        let mut dlq_forwarded = false;
        if matches!(action, AckAction::Nack) {
            let payload =
                if let Some(bytes) = data_kv.get(msg_id).await.map_err(|e| {
                    JetsError::Internal(format!("failed to read message bytes: {}", e))
                })? {
                    bytes
                } else {
                    serde_json::json!({
                        "msg_id": msg_id,
                        "reason": reason.unwrap_or("nack"),
                        "failed_at_ms": chrono::Utc::now().timestamp_millis(),
                    })
                    .to_string()
                    .into_bytes()
                    .into()
                };

            js.publish(self.subject_dlq.clone(), payload)
                .await
                .map_err(|e| JetsError::Transport(format!("failed to publish to dlq: {}", e)))?
                .await
                .map_err(|e| JetsError::Transport(format!("failed to ack dlq publish: {}", e)))?;
            dlq_forwarded = true;
        }

        Ok(AckResult {
            msg_id: msg_id.to_string(),
            new_state: new_state.to_string(),
            dlq_forwarded,
        })
    }

    pub async fn list_dlq(&self, filter: ListDlqFilter) -> Result<Vec<DlqMessage>, JetsError> {
        let js = self.js().await?;
        self.ensure_dlq_stream(&js).await?;
        let mut stream = js
            .get_stream(self.dlq_stream.clone())
            .await
            .map_err(|e| JetsError::Transport(format!("failed to get dlq stream: {}", e)))?;
        let info = stream
            .info()
            .await
            .map_err(|e| JetsError::Transport(format!("failed to get dlq info: {}", e)))?;

        let mut seq = filter.from_seq.unwrap_or(info.state.first_sequence);
        let end = info.state.last_sequence;
        if seq == 0 || end == 0 || seq > end {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        while seq <= end && out.len() < filter.limit {
            let raw = match stream.get_raw_message(seq).await {
                Ok(v) => v,
                Err(_) => {
                    seq += 1;
                    continue;
                }
            };
            if let Ok(envelope) = Envelope::decode(raw.payload.as_ref()) {
                out.push(DlqMessage {
                    stream_seq: seq,
                    envelope_b64: BASE64.encode(raw.payload.as_ref()),
                    envelope,
                });
            }
            seq += 1;
        }
        Ok(out)
    }

    pub async fn replay_dlq_message(
        &self,
        stream_seq: u64,
        replay_to: Option<&str>,
    ) -> Result<PublishResult, JetsError> {
        if stream_seq == 0 {
            return Err(JetsError::InvalidInput(
                "stream_seq must be > 0".to_string(),
            ));
        }

        let js = self.js().await?;
        self.ensure_dlq_stream(&js).await?;
        let stream = js
            .get_stream(self.dlq_stream.clone())
            .await
            .map_err(|e| JetsError::Transport(format!("failed to get dlq stream: {}", e)))?;

        let raw = stream
            .get_raw_message(stream_seq)
            .await
            .map_err(|e| JetsError::NotFound(format!("dlq message not found: {}", e)))?;

        let mut envelope = Envelope::decode(raw.payload.as_ref())
            .map_err(|e| JetsError::Internal(format!("invalid dlq envelope: {}", e)))?;

        if let Some(target) = replay_to {
            envelope.to = target.to_string();
        }
        envelope.sent_at = chrono::Utc::now().timestamp_millis() as u64;
        envelope.request_id = uuid::Uuid::new_v4().to_string();
        envelope.seq = 0;

        self.publish(PublishInput { envelope }).await
    }

    pub fn normalize_state_filter(input: Option<&str>) -> Result<Option<String>, JetsError> {
        let Some(v) = input else {
            return Ok(None);
        };
        let normalized = v.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Ok(None);
        }
        let allowed = ["unread", "acked", "failed", "expired"];
        if !allowed.contains(&normalized.as_str()) {
            return Err(JetsError::InvalidInput(format!(
                "invalid state '{}' expected one of {:?}",
                v, allowed
            )));
        }
        Ok(Some(normalized))
    }

    pub fn decode_envelope_b64(input: &str) -> Result<Envelope, JetsError> {
        let bytes = BASE64
            .decode(input)
            .map_err(|e| JetsError::InvalidInput(format!("invalid envelope_b64: {}", e)))?;
        Envelope::decode(bytes.as_slice())
            .map_err(|e| JetsError::InvalidInput(format!("invalid protobuf envelope: {}", e)))
    }

    pub fn payload_kind_name(kind: &jets_proto::payload::Kind) -> &'static str {
        match kind {
            jets_proto::payload::Kind::StateSnapshot(_) => "state_snapshot",
            jets_proto::payload::Kind::StatePatch(_) => "state_patch",
            jets_proto::payload::Kind::ExecCommand(_) => "exec_command",
            jets_proto::payload::Kind::FilePatch(_) => "file_patch",
            jets_proto::payload::Kind::EnvPatch(_) => "env_patch",
            jets_proto::payload::Kind::LifecycleAction(_) => "lifecycle_action",
            jets_proto::payload::Kind::Ack(_) => "ack",
            jets_proto::payload::Kind::Error(_) => "error",
        }
    }

    pub fn envelope_summary(envelope: &Envelope) -> HashMap<String, String> {
        let mut out = HashMap::new();
        out.insert("msg_id".to_string(), envelope.msg_id.clone());
        out.insert("from".to_string(), envelope.from.clone());
        out.insert("to".to_string(), envelope.to.clone());
        out.insert("request_id".to_string(), envelope.request_id.clone());
        out.insert(
            "permission_level".to_string(),
            format!("{}", envelope.permission_level),
        );
        if let Some(payload) = envelope.payload.as_ref() {
            if let Some(kind) = payload.kind.as_ref() {
                out.insert(
                    "payload_kind".to_string(),
                    Self::payload_kind_name(kind).to_string(),
                );
            }
        }
        out
    }
}
