use std::collections::HashMap;
use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use std::path::{Path, PathBuf};
use core::models::{RawRecord, RecordMetadata};
use core::errors::{AdapterError, AdapterResult};
use core::traits::Adapter;
use futures::stream::{BoxStream, Stream};
use futures::StreamExt;
use async_stream::stream;

#[derive(Debug, Clone)]
pub struct FileAdapterConfig {
    pub input_path: PathBuf,
    pub processed_path: Option<PathBuf>,
    pub error_path: Option<PathBuf>,
    pub file_pattern: Option<String>,  // Regex pattern for files
    pub recursive: bool,
}

pub struct FileAdapter {
    config: FileAdapterConfig,
}

impl FileAdapter {
    pub fn new(config: FileAdapterConfig) -> Self {
        Self { config }
    }

    async fn read_file(&self, path: &Path) -> AdapterResult<Vec<RawRecord>> {
        let file = File::open(path).await
            .map_err(|e| AdapterError::Io(e.to_string()))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut records = Vec::new();
        let mut line_num = 0;

        while let Some(line) = lines.next_line().await
            .map_err(|e| AdapterError::Io(e.to_string()))? {
            line_num += 1;

            records.push(RawRecord {
                data: line.into_bytes(),
                metadata: RecordMetadata {
                    source: path.to_string_lossy().to_string(),
                    line_number: Some(line_num),
                    offset: None,
                    timestamp: Some(chrono::Utc::now()),
                    custom: HashMap::new(),
                },
            });
        }

        Ok(records)
    }

    async fn collect_files(&self) -> AdapterResult<Vec<PathBuf>> {
        let path = &self.config.input_path;
        let mut files = Vec::new();

        if path.is_file() {
            files.push(path.clone());
        } else if path.is_dir() {
            self.walk_directory(path, &mut files).await?;
        }

        Ok(files)
    }

    async fn walk_directory(&self, dir: &Path, files: &mut Vec<PathBuf>) -> AdapterResult<()> {
        let mut read_dir = tokio::fs::read_dir(dir).await
            .map_err(|e| AdapterError::Io(e.to_string()))?;

        while let Some(entry) = read_dir.next_entry().await
            .map_err(|e| AdapterError::Io(e.to_string()))? {
            let path = entry.path();

            if path.is_file() {
                if let Some(pattern) = &self.config.file_pattern {
                    if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                        if regex::Regex::new(pattern)
                            .map_err(|e| AdapterError::Config(e.to_string()))?
                            .is_match(filename) {
                            files.push(path);
                        }
                    }
                } else {
                    files.push(path);
                }
            } else if path.is_dir() && self.config.recursive {
                Box::pin(self.walk_directory(&path, files)).await?;
            }
        }

        Ok(())
    }
    async fn stream_async(&self) -> AdapterResult<impl Stream<Item = AdapterResult<RawRecord>>> {
        let files = self.collect_files().await?;

        let stream = stream! {
            for file in files {
                let mut file_stream = match self.stream_file_async(&file).await {
                    Ok(stream) => stream,
                    Err(e) => {
                        yield Err(e);
                        continue;
                    }
                };

                while let Some(record) = file_stream.next().await {
                    yield record;
                }
            }
        };

        Ok(stream)
    }

    async fn stream_file_async(&self, path: &Path) -> AdapterResult<impl Stream<Item = AdapterResult<RawRecord>>> {
        let file = File::open(path).await
            .map_err(|e| AdapterError::Io(e.to_string()))?;

        let reader = BufReader::new(file);
        let source = path.to_string_lossy().to_string();

        let stream = stream! {
            let mut lines = reader.lines();
            let mut line_num = 0;

            while let Ok(Some(line)) = lines.next_line().await {
                line_num += 1;
                yield Ok(RawRecord {
                    data: line.into_bytes(),
                    metadata: RecordMetadata {
                        source: source.clone(),
                        line_number: Some(line_num),
                        offset: None,
                        timestamp: Some(chrono::Utc::now()),
                        custom: HashMap::new(),
                    },
                });
            }
        };

        Ok(stream)
    }
}

#[async_trait]
impl Adapter for FileAdapter {
    async fn read(&self) -> AdapterResult<Vec<RawRecord>> {
        let files = self.collect_files().await?;
        let mut all_records = Vec::new();

        for file in files {
            let records = self.read_file(&file).await?;
            all_records.extend(records);
        }

        Ok(all_records)
    }

    async fn stream(&self) -> AdapterResult<Box<dyn Stream<Item = AdapterResult<RawRecord>> + Send>> {
        Ok(Box::new(self.stream_async().await?))
    }

    fn config(&self) -> &dyn std::any::Any {
        &self.config
    }
}