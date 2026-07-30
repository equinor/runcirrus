use crate::queues::Scheduler;
use async_trait::async_trait;
use std::error::Error;
use std::process::Command;
use tokio::process::Command as AsyncCommand;

#[derive(Default)]
pub struct LocalScheduler {}

impl LocalScheduler {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Scheduler for LocalScheduler {
    fn name(&self) -> &'static str {
        "local"
    }

    async fn exec(&self, command: Command) -> Result<(), Box<dyn Error>> {
        let mut command = AsyncCommand::from(command);

        let mut child = command.spawn()?;
        child.wait().await?;
        Ok(())
    }
}
