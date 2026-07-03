//! Measure per-op latency of concurrent GETs on one multiplexed connection,
//! with TCP_NODELAY on or off — quantifies the Nagle cost of the default
//! `TcpSettings { nodelay: false }` under multiplexed concurrency.
//!
//! Usage: nagle_probe <redis-url> <tasks> <ops-per-task> <nodelay: true|false>

use redis::io::tcp::TcpSettings;
use redis::{AsyncCommands, IntoConnectionInfo};
use std::time::Instant;

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let mut args = std::env::args().skip(1);
    let url = args.next().expect("url");
    let tasks: usize = args.next().expect("tasks").parse().unwrap();
    let ops: usize = args.next().expect("ops").parse().unwrap();
    let nodelay: bool = args.next().expect("nodelay").parse().unwrap();

    let info = url
        .into_connection_info()?
        .set_tcp_settings(TcpSettings::default().set_nodelay(nodelay));
    let client = redis::Client::open(info)?;
    let con = client.get_multiplexed_async_connection().await?;

    // 128-byte value, small command/reply so segments stay well under one MSS.
    {
        let mut c = con.clone();
        let () = c.set("nagle:probe", vec![b'x'; 128]).await?;
        for _ in 0..1000 {
            let _: Vec<u8> = c.get("nagle:probe").await?; // warmup
        }
    }

    let mut handles = Vec::new();
    for _ in 0..tasks {
        let mut c = con.clone();
        handles.push(tokio::spawn(async move {
            let mut lat_us = Vec::with_capacity(ops);
            for _ in 0..ops {
                let t = Instant::now();
                let _: Vec<u8> = c.get("nagle:probe").await.unwrap();
                lat_us.push(t.elapsed().as_micros() as u64);
            }
            lat_us
        }));
    }

    let started = Instant::now();
    let mut all = Vec::with_capacity(tasks * ops);
    for h in handles {
        all.extend(h.await.unwrap());
    }
    let wall = started.elapsed();
    all.sort_unstable();
    let pct = |p: f64| all[((all.len() as f64 * p) as usize).min(all.len() - 1)];
    println!(
        "nodelay={nodelay} tasks={tasks} ops={} | p50={}us p99={}us p999={}us p9999={}us max={}us | {:.0} ops/s",
        all.len(),
        pct(0.50),
        pct(0.99),
        pct(0.999),
        pct(0.9999),
        all[all.len() - 1],
        all.len() as f64 / wall.as_secs_f64(),
    );
    Ok(())
}
