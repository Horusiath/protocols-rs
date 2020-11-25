use crate::Result;

#[derive(Debug)]
pub struct ReplicatedEventLog {

}

impl ReplicatedEventLog {
    pub async fn append<B: AsRef<[u8]>>(&mut self, buf: B) -> Result<()> {
        unimplemented!()
    }
}
