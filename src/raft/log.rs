use crate::Result;

#[derive(Debug)]
pub struct ReplicatedEventLog {

}

impl ReplicatedEventLog {
    pub async fn append_bytes(&mut self, buf: &[u8]) -> Result<()> {
        unimplemented!()
    }
}
