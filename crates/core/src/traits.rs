use async_trait::async_trait;
use futures::stream::Stream;
use crate::errors::AdapterResult;
use crate::models::{RawRecord, StructuredRecord};

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn read(&self) -> AdapterResult<Vec<RawRecord>>;
    async fn stream(&self) -> AdapterResult<Box<dyn Stream<Item = AdapterResult<RawRecord>> + Send>>;
    fn config(&self) -> &dyn std::any::Any;
}

#[async_trait]
pub trait Mapper<T = StructuredRecord>: Send + Sync {
    fn map(&self, raw: RawRecord) -> AdapterResult<T>;
    fn map_batch(&self, raws: Vec<RawRecord>) -> AdapterResult<Vec<T>> {
        raws.into_iter().map(|r| self.map(r)).collect()
    }
}