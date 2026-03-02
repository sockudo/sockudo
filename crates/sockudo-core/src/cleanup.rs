#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct CleanupConfig {
    pub queue_buffer_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub worker_threads: WorkerThreadsConfig,
    pub max_retry_attempts: u32,
    pub async_enabled: bool,
    pub fallback_to_sync: bool,
}

impl CleanupConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.queue_buffer_size == 0 {
            return Err("queue_buffer_size must be greater than 0".to_string());
        }

        if self.batch_size == 0 {
            return Err("batch_size must be greater than 0".to_string());
        }

        if self.batch_timeout_ms == 0 {
            return Err("batch_timeout_ms must be greater than 0".to_string());
        }

        if let WorkerThreadsConfig::Fixed(n) = self.worker_threads
            && n == 0
        {
            return Err("worker_threads must be greater than 0 when using fixed count".to_string());
        }

        if self.queue_buffer_size < self.batch_size {
            return Err(format!(
                "queue_buffer_size ({}) should be at least as large as batch_size ({})",
                self.queue_buffer_size, self.batch_size
            ));
        }

        if self.batch_timeout_ms > 60000 {
            return Err(format!(
                "batch_timeout_ms ({}) is unusually high (> 60 seconds), this may cause delays",
                self.batch_timeout_ms
            ));
        }

        if !self.async_enabled && !self.fallback_to_sync {
            return Err("Either async_enabled or fallback_to_sync must be true".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum WorkerThreadsConfig {
    Auto,
    Fixed(usize),
}

impl serde::Serialize for WorkerThreadsConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            WorkerThreadsConfig::Auto => serializer.serialize_str("auto"),
            WorkerThreadsConfig::Fixed(n) => serializer.serialize_u64(*n as u64),
        }
    }
}

impl<'de> serde::Deserialize<'de> for WorkerThreadsConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};

        struct WorkerThreadsVisitor;

        impl<'de> Visitor<'de> for WorkerThreadsVisitor {
            type Value = WorkerThreadsConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a positive integer or the string \"auto\"")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match value.to_lowercase().as_str() {
                    "auto" => Ok(WorkerThreadsConfig::Auto),
                    _ => match value.parse::<usize>() {
                        Ok(n) if n > 0 => Ok(WorkerThreadsConfig::Fixed(n)),
                        Ok(_) => Err(de::Error::custom("worker_threads must be greater than 0")),
                        Err(_) => Err(de::Error::custom(format!(
                            "expected \"auto\" or positive integer, got \"{}\"",
                            value
                        ))),
                    },
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value > 0 && value <= usize::MAX as u64 {
                    Ok(WorkerThreadsConfig::Fixed(value as usize))
                } else if value == 0 {
                    Err(de::Error::custom("worker_threads must be greater than 0"))
                } else {
                    Err(de::Error::custom("worker_threads value too large"))
                }
            }
        }

        deserializer.deserialize_any(WorkerThreadsVisitor)
    }
}

impl WorkerThreadsConfig {
    pub fn resolve(&self) -> usize {
        match self {
            WorkerThreadsConfig::Auto => {
                let cpu_count = num_cpus::get();
                let auto_threads = (cpu_count / 4).clamp(1, 4);
                tracing::info!(
                    "Auto-detected {} CPUs, using {} cleanup worker threads",
                    cpu_count,
                    auto_threads
                );
                auto_threads
            }
            WorkerThreadsConfig::Fixed(n) => *n,
        }
    }
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            queue_buffer_size: 50000,
            batch_size: 25,
            batch_timeout_ms: 50,
            worker_threads: WorkerThreadsConfig::Auto,
            max_retry_attempts: 2,
            async_enabled: true,
            fallback_to_sync: true,
        }
    }
}
