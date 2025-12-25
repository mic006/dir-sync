//! Hash computation, using blake3 algo

use std::path::PathBuf;

pub struct Hasher {
    sender_small: flume::Sender<PathBuf>,
    sender_big: flume::Sender<PathBuf>,
}
impl Hasher {
    pub fn new() -> Self {
        if let Ok(threads) = std::env::var("DBG_HASH_MMAP_THREADS") {
            let nb_threads = threads.parse::<u8>().unwrap();
            let (sender, receiver) = flume::unbounded();
            for _ in 0..nb_threads {
                tokio::task::spawn_blocking({
                    let receiver = receiver.clone();
                    || hash_mmap_task(receiver)
                });
            }
            Self {
                sender_small: sender.clone(),
                sender_big: sender,
            }
        } else if let Ok(threads) = std::env::var("DBG_HASH_MIX_THREADS") {
            let nb_threads = threads.parse::<u8>().unwrap();
            let (sender_small, receiver_small) = flume::unbounded();
            let (sender_big, receiver_big) = flume::unbounded();
            for _ in 0..nb_threads {
                tokio::task::spawn_blocking({
                    let receiver = receiver_small.clone();
                    || hash_mmap_task(receiver)
                });
            }
            tokio::task::spawn_blocking(|| hash_rayon_task(receiver_big));
            Self {
                sender_small,
                sender_big,
            }
        } else {
            let (sender, receiver) = flume::unbounded();
            tokio::task::spawn_blocking(|| hash_rayon_task(receiver));
            Self {
                sender_small: sender.clone(),
                sender_big: sender,
            }
        }
    }

    pub fn send(&self, p: PathBuf, size: u64) {
        if size < 128 * 1024 {
            self.sender_small.send(p).unwrap();
        } else {
            self.sender_big.send(p).unwrap();
        }
    }
}

fn hash_rayon_task(recv: flume::Receiver<PathBuf>) -> anyhow::Result<()> {
    while let Ok(p) = recv.recv() {
        let mut hasher = blake3::Hasher::new();
        hasher.update_mmap_rayon(&p)?;
        println!("{}  {}", hasher.finalize(), p.display());
    }
    println!("thread end");
    Ok(())
}

fn hash_mmap_task(recv: flume::Receiver<PathBuf>) -> anyhow::Result<()> {
    while let Ok(p) = recv.recv() {
        let mut hasher = blake3::Hasher::new();
        hasher.update_mmap(&p)?;
        println!("{}  {}", hasher.finalize(), p.display());
    }
    println!("thread end");
    Ok(())
}
