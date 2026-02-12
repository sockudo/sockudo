use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use pushers::{Channel, Config, Pusher};
use sonic_rs::json;
use tokio::time::{MissedTickBehavior, interval_at};

#[derive(Debug, Parser)]
#[command(
    name = "send",
    about = "Benchmark sender for Sockudo/Pusher-compatible HTTP API"
)]
struct Args {
    /// Specify at which interval to send each message (seconds).
    #[arg(short = 'i', long = "interval", default_value_t = 0.1)]
    interval: f64,

    /// Specify the number of messages to send (unlimited if omitted).
    #[arg(short = 'm', long = "messages")]
    messages: Option<u64>,

    /// Specify the host to connect to.
    #[arg(short = 'h', long = "host", default_value = "127.0.0.1")]
    host: String,

    /// Specify the ID to use.
    #[arg(long = "app-id", default_value = "app-id")]
    app_id: String,

    /// Specify the key to use.
    #[arg(long = "app-key", default_value = "app-key")]
    app_key: String,

    /// Specify the secret to use.
    #[arg(long = "app-secret", default_value = "app-secret")]
    app_secret: String,

    /// Specify the port to connect to.
    #[arg(short = 'p', long = "port", default_value_t = 6001)]
    port: u16,

    /// Securely connect to the server.
    #[arg(short = 's', long = "ssl", default_value_t = false)]
    ssl: bool,

    /// Enable verbosity.
    #[arg(short = 'v', long = "verbose", default_value_t = false)]
    verbose: bool,
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as i64
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if !(args.interval.is_finite() && args.interval > 0.0) {
        return Err("--interval must be a positive finite number".into());
    }

    let config = Config::builder()
        .app_id(args.app_id)
        .key(args.app_key)
        .secret(args.app_secret)
        .host(args.host)
        .port(args.port)
        .use_tls(args.ssl)
        .build()?;

    let pusher = Pusher::new(config)?;
    let channels = [Channel::from_string("benchmark")?];

    let tick_every = Duration::from_secs_f64(args.interval);
    let start_at = tokio::time::Instant::now() + tick_every;
    let mut ticker = interval_at(start_at, tick_every);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut total_messages: u64 = 0;

    loop {
        ticker.tick().await;

        if let Some(max_messages) = args.messages
            && total_messages >= max_messages
        {
            println!("Sent: {total_messages} messages");
            break;
        }

        let time = now_unix_millis();

        pusher
            .trigger(
                &channels,
                "timed-message",
                json!({ "time": time }),
                None,
            )
            .await?;

        total_messages += 1;

        if args.verbose {
            println!("Sent message with time: {time}");
        }
    }

    Ok(())
}
