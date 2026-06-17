/// Optional TCP metrics exporter settings.
#[derive(Debug, Clone)]
pub struct TcpExporterOptions {
    /// Host or IP address for the TCP exporter listener.
    pub host: String,
    /// TCP exporter listener port.
    pub port: u16,
    /// Internal and per-client buffer size. `None` makes the exporter unbounded.
    pub buffer_size: Option<usize>,
}
