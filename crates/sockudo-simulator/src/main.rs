use clap::Parser;
use sockudo_simulator::{DeterministicSimulator, FaultConfig, SimulatorConfig};

#[derive(Debug, Parser)]
#[command(
    name = "sockudo-sim",
    about = "Run Sockudo's deterministic indestructibility simulator"
)]
struct Cli {
    /// Replay seed. If omitted, a random seed is printed at startup.
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, default_value_t = 5_000)]
    ticks: u64,
    #[arg(long, default_value_t = 5)]
    nodes: usize,
    #[arg(long, default_value_t = 8)]
    clients: usize,
    #[arg(long, default_value_t = 4)]
    channels: usize,
    #[arg(long, default_value_t = 16)]
    users: usize,
    #[arg(long, default_value_t = 25)]
    oracle_every: u64,
    #[arg(long, default_value_t = 7)]
    page_limit: usize,
    #[arg(long)]
    history_retention_messages: Option<usize>,
    #[arg(long)]
    presence_retention_events: Option<usize>,
    #[arg(long, default_value_t = 0.08)]
    drop_prob: f64,
    #[arg(long, default_value_t = 0.03)]
    duplicate_prob: f64,
    #[arg(long, default_value_t = 12)]
    max_delay_ticks: u64,
    #[arg(long, default_value_t = 0.002)]
    crash_prob: f64,
    #[arg(long, default_value_t = 0.020)]
    restart_prob: f64,
    #[arg(long, default_value_t = 0.003)]
    partition_prob: f64,
    #[arg(long, default_value_t = 0.020)]
    heal_prob: f64,
    #[arg(long, default_value_t = 0.0005)]
    stream_reset_prob: f64,
    /// Emit the final report as JSON.
    #[arg(long)]
    json: bool,
}

impl Cli {
    fn into_config(self) -> (SimulatorConfig, bool) {
        let seed = self.seed.unwrap_or_else(rand::random);
        let config = SimulatorConfig {
            seed,
            ticks: self.ticks,
            nodes: self.nodes,
            clients: self.clients,
            channels: self.channels,
            users: self.users,
            oracle_every: self.oracle_every,
            page_limit: self.page_limit,
            history_retention_messages: self.history_retention_messages.or(Some(512)),
            presence_retention_events: self.presence_retention_events.or(Some(512)),
            fault: FaultConfig {
                fanout_drop_probability: self.drop_prob,
                fanout_duplicate_probability: self.duplicate_prob,
                max_fanout_delay_ticks: self.max_delay_ticks,
                node_crash_probability: self.crash_prob,
                node_restart_probability: self.restart_prob,
                node_partition_probability: self.partition_prob,
                node_heal_probability: self.heal_prob,
                stream_reset_probability: self.stream_reset_prob,
            },
        };
        (config, self.json)
    }
}

#[tokio::main]
async fn main() {
    let (config, json) = Cli::parse().into_config();
    println!(
        "sockudo-sim: seed={} ticks={} nodes={} clients={} channels={}",
        config.seed, config.ticks, config.nodes, config.clients, config.channels
    );

    let seed = config.seed;
    let mut simulator = match DeterministicSimulator::new(config) {
        Ok(simulator) => simulator,
        Err(error) => {
            eprintln!("sockudo-sim: invalid configuration: {error}");
            std::process::exit(2);
        }
    };

    match simulator.run().await {
        Ok(report) => {
            if json {
                match serde_json::to_string_pretty(&report) {
                    Ok(encoded) => println!("{encoded}"),
                    Err(error) => {
                        eprintln!("sockudo-sim: failed to encode report: {error}");
                        std::process::exit(1);
                    }
                }
            } else {
                println!(
                    "sockudo-sim: OK seed={} ticks={} operations={} oracles={} history_commits={} version_commits={} presence_events={} recovered={} dropped={} duplicated={} resets={}",
                    report.seed,
                    report.ticks,
                    report.operations,
                    report.oracle_checks,
                    report.history_commits,
                    report.version_commits,
                    report.presence_events,
                    report.recovered_messages,
                    report.dropped_fanout,
                    report.duplicated_fanout,
                    report.stream_resets,
                );
            }
        }
        Err(error) => {
            eprintln!("sockudo-sim: FAILED - reproduce with --seed {seed}\n{error}");
            std::process::exit(1);
        }
    }
}
