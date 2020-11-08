use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Serialize, Deserialize, Deserializer, Serializer};
use serde::de::Visitor;
use smallvec::alloc::fmt::Formatter;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::Clock;

type Ticks = u64;

pub static LATEST: AtomicU64 = AtomicU64::new(0);
pub const SYS_MASK: Ticks = !0x0f;

/// Hybrid logical time. It stores an approximate (never decreasing) value of system time, that
/// can be safely used to compare date of occurrence of two events. It can also be serialized and
/// deserialized.
///
/// Call `HybridTime::now()` to receive current time.
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct HybridTime(Ticks);

impl HybridTime {

    fn sys_time() -> Ticks {
        let sys = SystemTime::now();
        let d = sys.duration_since(UNIX_EPOCH).unwrap();
        (d.as_nanos() as u64) & SYS_MASK
    }

    pub fn now() -> Self {
        loop {
            let ticks = LATEST.load(Ordering::Relaxed);
            let current = Self::sys_time();
            let latest = current.max(ticks) + 1;
            if LATEST.compare_and_swap(ticks, latest, Ordering::AcqRel) == ticks {
                return HybridTime(latest)
            }
        }
    }

    pub fn sync(remote: HybridTime) {
        loop {
            let ticks = LATEST.load(Ordering::Relaxed);
            let latest = ticks.max(remote.0);
            if LATEST.compare_and_swap(ticks, latest, Ordering::AcqRel) == ticks {
                break;
            }
        }
    }
}

impl Into<SystemTime> for HybridTime {
    fn into(self) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_nanos(self.0)
    }
}

impl Serialize for HybridTime {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for HybridTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> where
        D: Deserializer<'de> {

        struct InstantVisitor;
        impl<'de> Visitor<'de> for InstantVisitor {
            type Value = u64;

            fn expecting(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "Instant")
            }
        }

        let ticks = deserializer.deserialize_u64(InstantVisitor)?;
        let time = HybridTime(ticks);
        HybridTime::sync(time);
        Ok(time)
    }
}

impl Clock for HybridTime {
    #[inline]
    fn now() -> Self {
        HybridTime::now()
    }
}