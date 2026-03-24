use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawRecord {
    pub data: Vec<u8>,  // Raw bytes so it can be passed to mapper
    pub metadata: RecordMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordMetadata {
    pub source: String,
    pub line_number: Option<usize>,
    pub offset: Option<u64>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub custom: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct StructuredRecord {
    pub id: Option<String>,
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    pub fields: HashMap<String, serde_json::Value>,
}
