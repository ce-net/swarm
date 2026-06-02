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
            let pool = candidates(ce.atlas().await?, &select);
            if pool.is_empty() {
                bail!("no matching hosts in the atlas (need 'docker'{})", sel_note(&select));
            }

            // Trust-tier the placement: rank candidates by delivered work (reputation read
            // from each host's on-chain interaction history), so proven hosts get the work
            // first. Strangers sort last. Apps can swap in a richer trust model.
            let mut scored: Vec<(u64, AtlasEntry)> = Vec::new();
            for h in pool {
                let rep = ce.history(&h.node_id).await.map(|r| r.delivered_work()).unwrap_or(0);
                scored.push((rep, h));
            }
            scored.sort_by(|a, b| b.0.cmp(&a.0));
            let hosts: Vec<(u64, AtlasEntry)> = scored.into_iter().take(count).collect();

            println!("Fanning '{}' out to {} host(s) (most-proven first)...\n", command.join(" "), hosts.len());
            let hosts: Vec<AtlasEntry> = hosts
                .into_iter()
                .map(|(rep, h)| {
                    println!("  {} (delivered {rep} jobs)", &h.node_id[..h.node_id.len().min(12)]);
                    h
                })
                .collect();
            println!();

            // Scatter: dispatch to every host concurrently.
            let mut set = JoinSet::new();
            for h in hosts {
                let ce = ce.clone();
                let image = image.clone();
                let command = command.clone();
                let node_id = h.node_id.clone();
                set.spawn(async move {
                    let out = ce.mesh_exec(&node_id, &image, &command).await;
                    (node_id, out)
                });
            }

            // Gather: collect each result as it lands.
            let mut ok = 0usize;
            let mut failed = 0usize;
            while let Some(joined) = set.join_next().await {
                let (node_id, out) = joined?;
                let short = &node_id[..node_id.len().min(8)];
                match out {
                    Ok(r) if r.ok() => {
                        ok += 1;
                        println!("[{short}] exit 0\n{}", indent(r.stdout.trim_end()));
                    }
                    Ok(r) => {
                        failed += 1;
                        println!("[{short}] exit {}\n{}", r.exit_code, indent(r.stderr.trim_end()));
                    }
                    Err(e) => {
                        failed += 1;
                        println!("[{short}] dispatch failed: {e}");
                    }
                }
            }
            println!("\n{ok} ok, {failed} failed.");
            if ok == 0 {
                bail!("no host returned a successful result");
            }
        }
    }
    Ok(())
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
