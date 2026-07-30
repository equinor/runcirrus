mod local;

pub use crate::queues::local::LocalScheduler;
use async_trait::async_trait;
use std::error::Error;
use std::process::Command;

#[async_trait]
pub trait Scheduler {
    fn name(&self) -> &'static str;

    async fn exec(&self, command: Command) -> Result<(), Box<dyn Error>>;
}
