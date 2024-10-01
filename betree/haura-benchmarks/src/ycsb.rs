//! Benchmarks based on the YCSB-{A,B,C,D,E} workloads.
//!
//! Link: https://web.archive.org/web/20170809211159id_/http://www.cs.toronto.edu/~delara/courses/csc2231/papers/cooper.pdf

use betree_perf::KvClient;
use rand::distributions::Distribution;
use rand::prelude::SliceRandom;
use rand::{SeedableRng, Rng};
use std::io::Write;

// Default in YCSB, 10 x 100 bytes field in one struct.
const ENTRY_SIZE: usize = 1000;
// Default of YCSB
const ZIPF_EXP: f64 = 0.99;

/// Operations: Read/Write
/// Distribution: Zipfian
/// Access Size: 1000 bytes
pub fn run(mut client: KvClient, size: u64, threads: usize, runtime: u64, ratio: f64, id: &str) {
    println!("Running YCSB Workload");
    println!("Threads: {threads}\t Runtime: {runtime}s\t R/W ratio: {ratio}");
    println!("Filling KV store...");
    let mut keys = client.fill_entries(size / ENTRY_SIZE as u64, ENTRY_SIZE as u32);
    keys.shuffle(client.rng());
    println!("Creating distribution...");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("ycsb_{id}.csv"))
        .unwrap();
    let mut w = std::io::BufWriter::new(f);
    w.write_all(b"threads,ops,time_ns\n").unwrap();

    for workers in 1..=threads {
        println!("Running benchmark with {workers} threads...");
        let threads = (0..workers)
            .map(|_| std::sync::mpsc::channel::<std::time::Instant>())
            .enumerate()
            .map(|(id, (tx, rx))| {
                let keys = keys.clone();
                let ds = client.ds.clone();
                (
                    std::thread::spawn(move || {
                        let mut rng = rand_xoshiro::Xoshiro256Plus::seed_from_u64(id as u64);
                        let dist = zipf::ZipfDistribution::new(keys.len(), ZIPF_EXP).unwrap();
                        let buf = vec![1; ENTRY_SIZE];
                        let mut total = 0;
                        while let Ok(start) = rx.recv() {
                            while start.elapsed().as_secs() < runtime {
                                for _ in 0..100 {
                                    if rng.gen_bool(ratio) {
                                        ds.get(&keys[dist.sample(&mut rng) - 1][..])
                                            .unwrap()
                                            .unwrap();
                                    } else {
                                        ds.insert(&keys[dist.sample(&mut rng) - 1][..], &buf);
                                    }
                                    total += 1;
                                }
                            }
                        }
                        total
                    }),
                    tx,
                )
            })
            .collect::<Vec<_>>();
        let start = std::time::Instant::now();
        for (_t, tx) in threads.iter() {
            tx.send(start).unwrap();
        }
        let mut total = 0;
        for (t, tx) in threads.into_iter() {
            drop(tx);
            total += t.join().unwrap();
        }
        let end = start.elapsed();
        w.write_fmt(format_args!("{workers},{total},{}\n", end.as_nanos()))
            .unwrap();
        w.flush().unwrap();
        println!("Achieved: {} ops/sec", total as f32 / end.as_secs_f32());
        println!("          {} ns avg", end.as_nanos() / total);
    }
}
