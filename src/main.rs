//! swarm — a distributed work scheduler on the CE compute mesh.
//!
//! The first app on top of CE. It is a *client*: it discovers hosts from the node's atlas,
//! fans a task out across them over the mesh, and gathers the results. CE provides the
//! substrate (placement, sandboxed run, billing, the immutable interaction history); swarm
//! is the orchestration policy on top. See `ce/docs/apps/scheduler.md`.
//!
//! v0 is scatter/gather for one-shot commands (via `mesh_exec`, which returns output
//! synchronously). Directed long-running deploy, trust-tiered placement, verification dials,
//! and coordinator HA are the documented next steps.

use anyhow::{bail, Result};
use ce_rs::{AtlasEntry, CeClient};
use clap::{Parser, Subcommand};
use tokio::task::JoinSet;

#[derive(Parser)]
#[command(name = "swarm", about = "Distributed work scheduler on the CE mesh", version)]
struct Cli {
    /// CE node HTTP API URL.
    #[arg(long, default_value = "http://127.0.0.1:8844", global = true)]
    node: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List candidate hosts from the atlas (those able to run containers).
    Hosts {
        /// Only show hosts advertising this capability self-tag (e.g. gpu).
        #[arg(long)]
        select: Option<String>,
    },
    /// Fan a one-shot command out across N hosts and gather the results.
    ///
    /// Example: swarm run alpine:latest -n 8 --select gpu -- nvidia-smi
    Run {
        /// Container image to run on each host.
        image: String,
        /// Only place on hosts advertising this capability self-tag.
        #[arg(long)]
        select: Option<String>,
        /// Maximum number of hosts to fan out to.
        #[arg(short = 'n', long, default_value = "4")]
        count: usize,
        /// The command to run inside the container (after `--`).
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Run an identical command redundantly on K hosts and verify they agree.
    ///
    /// The verification dial for deterministic work on untrusted hosts: K independent runs,
    /// outputs compared. Unanimous = verified; divergence = a host lied (the minority is
    /// suspect). Example: swarm verify alpine:latest -k 3 -- sha256sum /etc/os-release
    Verify {
        image: String,
        #[arg(long)]
        select: Option<String>,
        /// Redundancy factor — how many independent hosts must run and agree.
        #[arg(short = 'k', long = "replicas", default_value = "3")]
        replicas: usize,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
}

/// Hosts that can run containers, optionally filtered by a capability self-tag.
fn candidates(atlas: Vec<AtlasEntry>, select: &Option<String>) -> Vec<AtlasEntry> {
    atlas
        .into_iter()
        // A host must advertise `docker` to run a cell.
        .filter(|h| h.has_tag("docker"))
        .filter(|h| select.as_ref().is_none_or(|t| h.has_tag(t)))
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ce = CeClient::new(cli.node);

    match cli.cmd {
        Cmd::Hosts { select } => {
            let hosts = candidates(ce.atlas().await?, &select);
            if hosts.is_empty() {
                println!("No candidate hosts (need a node advertising 'docker'{}).", sel_note(&select));
                return Ok(());
            }
            println!("{:<66}  {:>4}  {:>8}  {:>4}  tags", "NODE", "CPU", "MEM(MB)", "JOBS");
            for h in &hosts {
                println!(
                    "{:<66}  {:>4}  {:>8}  {:>4}  {}",
                    h.node_id, h.cpu_cores, h.mem_mb, h.running_jobs, h.tags.join(",")
                );
            }
        }

        Cmd::Run { image, select, count, command } => {
            if command.is_empty() {
                bail!("provide a command to run, e.g. swarm run alpine:latest -- echo hi");
            }
            let hosts = select_hosts(&ce, &select, count).await?;
            println!("Fanning '{}' out to {} host(s) (most-proven first)...\n", command.join(" "), hosts.len());
            for (rep, h) in &hosts {
                println!("  {} (delivered {rep})", short(&h.node_id));
            }
            println!();

            let results = scatter(&ce, &hosts, &image, &command).await?;
            let (mut ok, mut failed) = (0usize, 0usize);
            for (node_id, out) in results {
                match out {
                    Ok(r) if r.ok() => {
                        ok += 1;
                        println!("[{}] exit 0\n{}", short(&node_id), indent(r.stdout.trim_end()));
                    }
                    Ok(r) => {
                        failed += 1;
                        println!("[{}] exit {}\n{}", short(&node_id), r.exit_code, indent(r.stderr.trim_end()));
                    }
                    Err(e) => {
                        failed += 1;
                        println!("[{}] dispatch failed: {e}", short(&node_id));
                    }
                }
            }
            println!("\n{ok} ok, {failed} failed.");
            if ok == 0 {
                bail!("no host returned a successful result");
            }
        }

        Cmd::Verify { image, select, replicas, command } => {
            if command.is_empty() {
                bail!("provide a command to verify, e.g. swarm verify alpine:latest -- sha256sum /etc/hostname");
            }
            if replicas < 2 {
                bail!("--replicas must be >= 2 for verification (use `run` for a single host)");
            }
            let hosts = select_hosts(&ce, &select, replicas).await?;
            if hosts.len() < replicas {
                eprintln!(
                    "note: only {} matching host(s) available; verifying with {} of the requested {replicas}.",
                    hosts.len(),
                    hosts.len()
                );
            }
            println!("Verifying '{}' across {} host(s)...\n", command.join(" "), hosts.len());

            let results = scatter(&ce, &hosts, &image, &command).await?;

            // Group successful runs by their (stdout, exit) — the "answer" each host returned.
            let mut groups: std::collections::HashMap<(String, i64), Vec<String>> = std::collections::HashMap::new();
            let mut errors = 0usize;
            for (node_id, out) in results {
                match out {
                    Ok(r) => groups.entry((r.stdout.trim_end().to_string(), r.exit_code)).or_default().push(node_id),
                    Err(e) => {
                        errors += 1;
                        println!("[{}] dispatch failed: {e}", short(&node_id));
                    }
                }
            }

            let mut groups: Vec<((String, i64), Vec<String>)> = groups.into_iter().collect();
            groups.sort_by(|a, b| b.1.len().cmp(&a.1.len())); // majority first
            let agreeing = groups.first().map(|g| g.1.len()).unwrap_or(0);
            let total: usize = groups.iter().map(|g| g.1.len()).sum();

            if groups.len() == 1 && errors == 0 {
                let ((stdout, code), hosts) = &groups[0];
                println!("✓ VERIFIED — {}/{} hosts agree (exit {code}):\n{}", hosts.len(), hosts.len(), indent(stdout));
            } else {
                println!("⚠ DIVERGENCE — {} distinct result(s) across {total} host(s):\n", groups.len());
                for (i, ((stdout, code), nodes)) in groups.iter().enumerate() {
                    let tag = if i == 0 { "majority" } else { "MINORITY (suspect)" };
                    let who: Vec<String> = nodes.iter().map(|n| short(n)).collect();
                    println!("  result {} — {} host(s) [{}] (exit {code}, {tag}):\n{}\n", i + 1, nodes.len(), who.join(","), indent(stdout));
                }
                println!("Majority result has {agreeing}/{total} agreement.");
                if groups.len() > 1 {
                    bail!("results diverged — at least one host returned a different answer");
                }
            }
        }
    }
    Ok(())
}

/// Discover docker-capable hosts matching `select`, ranked by on-chain delivered work
/// (most-proven first), truncated to `count`. Returns (delivered_work, host).
async fn select_hosts(
    ce: &CeClient,
    select: &Option<String>,
    count: usize,
) -> Result<Vec<(u64, AtlasEntry)>> {
    let pool = candidates(ce.atlas().await?, select);
    if pool.is_empty() {
        bail!("no matching hosts in the atlas (need 'docker'{})", sel_note(select));
    }
    let mut scored: Vec<(u64, AtlasEntry)> = Vec::new();
    for h in pool {
        let rep = ce.history(&h.node_id).await.map(|r| r.delivered_work()).unwrap_or(0);
        scored.push((rep, h));
    }
    scored.sort_by(|a, b| b.0.cmp(&a.0));
    scored.truncate(count);
    Ok(scored)
}

/// Run `command` in `image` on every host concurrently; return (node_id, result) pairs.
async fn scatter(
    ce: &CeClient,
    hosts: &[(u64, AtlasEntry)],
    image: &str,
    command: &[String],
) -> Result<Vec<(String, Result<ce_rs::ExecResult>)>> {
    let mut set = JoinSet::new();
    for (_, h) in hosts {
        let ce = ce.clone();
        let image = image.to_string();
        let command = command.to_vec();
        let node_id = h.node_id.clone();
        set.spawn(async move {
            let out = ce.mesh_exec(&node_id, &image, &command).await;
            (node_id, out)
        });
    }
    let mut out = Vec::new();
    while let Some(joined) = set.join_next().await {
        out.push(joined?);
    }
    Ok(out)
}

fn short(node_id: &str) -> String {
    node_id[..node_id.len().min(12)].to_string()
}

fn sel_note(select: &Option<String>) -> String {
    select.as_ref().map(|t| format!(" + '{t}'")).unwrap_or_default()
}

fn indent(s: &str) -> String {
    if s.is_empty() {
        return "    (no output)".into();
    }
    s.lines().map(|l| format!("    {l}")).collect::<Vec<_>>().join("\n")
}
