use clap::Parser;
use sockudo_simulator::{
    ActionWeights, DeterministicSimulator, FaultConfig, PushLabConfig, SimulationReport,
    SimulatorConfig, WorkloadConfig,
};
use std::path::PathBuf;

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
    #[arg(long, default_value_t = 35)]
    weight_publish: u32,
    #[arg(long, default_value_t = 15)]
    weight_create_versioned: u32,
    #[arg(long, default_value_t = 20)]
    weight_mutate_versioned: u32,
    #[arg(long, default_value_t = 20)]
    weight_presence: u32,
    #[arg(long, default_value_t = 7)]
    weight_recovery: u32,
    #[arg(long, default_value_t = 2)]
    weight_purge: u32,
    #[arg(long, default_value_t = 1)]
    weight_oracle: u32,
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
    #[arg(long, default_value_t = 0.002)]
    pause_prob: f64,
    #[arg(long, default_value_t = 0.030)]
    resume_prob: f64,
    #[arg(long, default_value_t = 0.003)]
    partition_prob: f64,
    #[arg(long, default_value_t = 0.020)]
    heal_prob: f64,
    #[arg(long, default_value_t = 0.003)]
    slow_prob: f64,
    #[arg(long, default_value_t = 0.002)]
    stale_prob: f64,
    #[arg(long, default_value_t = 0.0005)]
    stream_reset_prob: f64,
    /// Emit the final report as JSON.
    #[arg(long)]
    json: bool,
    /// Replay a JSON corpus. Accepts either `[seed, ...]` or `[SimulatorConfig, ...]`.
    #[arg(long)]
    corpus_file: Option<PathBuf>,
    #[arg(long, default_value_t = 8)]
    weight_push_register: u32,
    #[arg(long, default_value_t = 3)]
    weight_push_delete: u32,
    #[arg(long, default_value_t = 8)]
    weight_push_subscribe: u32,
    #[arg(long, default_value_t = 5)]
    weight_push_unsubscribe: u32,
    #[arg(long, default_value_t = 12)]
    weight_push_publish: u32,
    #[arg(long, default_value_t = 4)]
    weight_push_scheduled: u32,
    #[arg(long, default_value_t = 2)]
    weight_push_feedback: u32,
    #[arg(long, default_value_t = 2)]
    weight_push_repair: u32,
    #[arg(long, default_value_t = 32)]
    push_devices: usize,
    #[arg(long, default_value_t = 4)]
    push_max_retries: u32,
    #[arg(long, default_value_t = 17)]
    push_repair_every: u64,
    #[arg(long, default_value_t = 0.015)]
    queue_produce_lost_prob: f64,
    #[arg(long, default_value_t = 0.010)]
    queue_ack_lost_prob: f64,
    #[arg(long, default_value_t = 0.010)]
    queue_lease_timeout_prob: f64,
    #[arg(long, default_value_t = 0.006)]
    write_fail_before_commit_prob: f64,
    #[arg(long, default_value_t = 0.004)]
    write_fail_after_commit_prob: f64,
    #[arg(long, default_value_t = 0.006)]
    response_lost_prob: f64,
    #[arg(long, default_value_t = 0.010)]
    read_stale_prob: f64,
    #[arg(long, default_value_t = 0.100)]
    provider_retryable_prob: f64,
    #[arg(long, default_value_t = 0.040)]
    provider_reject_prob: f64,
    #[arg(long, default_value_t = 0.025)]
    provider_invalid_token_prob: f64,
    #[arg(long, default_value_t = 0.020)]
    provider_lost_response_prob: f64,
}

impl Cli {
    fn into_config(self) -> (SimulatorConfig, bool, Option<PathBuf>) {
        let seed = self.seed.unwrap_or_else(rand::random);
        let corpus_file = self.corpus_file.clone();
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
            workload: WorkloadConfig {
                weights: ActionWeights {
                    publish_message: self.weight_publish,
                    create_versioned_message: self.weight_create_versioned,
                    mutate_versioned_message: self.weight_mutate_versioned,
                    presence_transition: self.weight_presence,
                    recovery_probe: self.weight_recovery,
                    purge_history: self.weight_purge,
                    push_register_device: self.weight_push_register,
                    push_delete_device: self.weight_push_delete,
                    push_subscribe: self.weight_push_subscribe,
                    push_unsubscribe: self.weight_push_unsubscribe,
                    push_publish: self.weight_push_publish,
                    push_scheduled_publish: self.weight_push_scheduled,
                    push_provider_feedback: self.weight_push_feedback,
                    push_repair: self.weight_push_repair,
                    oracle_check: self.weight_oracle,
                },
            },
            fault: FaultConfig {
                fanout_drop_probability: self.drop_prob,
                fanout_duplicate_probability: self.duplicate_prob,
                max_fanout_delay_ticks: self.max_delay_ticks,
                node_crash_probability: self.crash_prob,
                node_restart_probability: self.restart_prob,
                node_pause_probability: self.pause_prob,
                node_resume_probability: self.resume_prob,
                node_partition_probability: self.partition_prob,
                node_heal_probability: self.heal_prob,
                node_slow_probability: self.slow_prob,
                node_stale_probability: self.stale_prob,
                stream_reset_probability: self.stream_reset_prob,
            },
            push: PushLabConfig {
                devices: self.push_devices,
                max_retries: self.push_max_retries,
                repair_every_ticks: self.push_repair_every,
                max_queue_delay_ticks: self.max_delay_ticks,
                max_provider_delay_ticks: self.max_delay_ticks,
                queue_produce_lost_probability: self.queue_produce_lost_prob,
                queue_duplicate_probability: self.duplicate_prob,
                queue_ack_lost_probability: self.queue_ack_lost_prob,
                queue_lease_timeout_probability: self.queue_lease_timeout_prob,
                backend_outage_probability: self.partition_prob / 2.0,
                backend_recovery_probability: self.heal_prob.max(0.001),
                write_fail_before_commit_probability: self.write_fail_before_commit_prob,
                write_fail_after_commit_probability: self.write_fail_after_commit_prob,
                response_lost_probability: self.response_lost_prob,
                read_stale_probability: self.read_stale_prob,
                provider_retryable_probability: self.provider_retryable_prob,
                provider_permanent_rejection_probability: self.provider_reject_prob,
                provider_invalid_token_probability: self.provider_invalid_token_prob,
                provider_lost_response_probability: self.provider_lost_response_prob,
                provider_delayed_result_probability: self.duplicate_prob.max(0.01),
                provider_duplicate_result_probability: self.duplicate_prob,
            },
        };
        (config, self.json, corpus_file)
    }
}

#[tokio::main]
async fn main() {
    let (config, json, corpus_file) = Cli::parse().into_config();
    if let Some(path) = corpus_file {
        match run_corpus(config, path, json).await {
            Ok(()) => return,
            Err(error) => {
                eprintln!("sockudo-sim: corpus failed\n{error}");
                std::process::exit(1);
            }
        }
    }
    run_one(config, json).await;
}

async fn run_one(config: SimulatorConfig, json: bool) {
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
                    "sockudo-sim: OK seed={} ticks={} operations={} oracles={} history_commits={} version_commits={} presence_events={} push_publishes={} push_results={} push_repairs={} recovered={} dropped={} duplicated={} resets={}",
                    report.seed,
                    report.ticks,
                    report.operations,
                    report.oracle_checks,
                    report.history_commits,
                    report.version_commits,
                    report.presence_events,
                    report.push.accepted_publishes,
                    report.push.delivery_results,
                    report.push.repair_requeued,
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

async fn run_corpus(
    base: SimulatorConfig,
    path: PathBuf,
    json: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(&path)?;
    let configs = match serde_json::from_str::<CorpusSpec>(&contents)? {
        CorpusSpec::Seeds(seeds) => seeds
            .into_iter()
            .map(|seed| {
                let mut config = base.clone();
                config.seed = seed;
                config
            })
            .collect::<Vec<_>>(),
        CorpusSpec::Configs(configs) => configs,
    };
    let mut reports = Vec::with_capacity(configs.len());
    for config in configs {
        let seed = config.seed;
        let mut simulator = DeterministicSimulator::new(config)?;
        match simulator.run().await {
            Ok(report) => reports.push(report),
            Err(error) => {
                return Err(format!("seed {seed} failed: {error}").into());
            }
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    } else {
        print_corpus_summary(&reports);
    }
    Ok(())
}

fn print_corpus_summary(reports: &[SimulationReport]) {
    println!("sockudo-sim: corpus OK entries={}", reports.len());
    for report in reports {
        println!(
            "  seed={} ticks={} ops={} push_publishes={} push_results={} repairs={}",
            report.seed,
            report.ticks,
            report.operations,
            report.push.accepted_publishes,
            report.push.delivery_results,
            report.push.repair_requeued,
        );
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum CorpusSpec {
    Seeds(Vec<u64>),
    Configs(Vec<SimulatorConfig>),
}
