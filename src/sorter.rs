use std::sync::{atomic::AtomicBool, Arc};

use bytemuck::Pod;
use crossbeam_channel::{bounded, unbounded, Receiver, Select, Sender};
use log::{debug, warn};

use crate::{
    chunk::{FileChunk, FileChunkDir, MemChunk},
    merge::merge_chunks_with_binary_heap,
    Result,
};

pub struct SortConfig {
    pub(crate) max_chunk_bytes: usize,
    pub(crate) concurrency: usize,
    pub(crate) merge_k: usize,
    pub(crate) canceled: AtomicBool,
}

impl Default for SortConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 1 << 30,
            concurrency: 8,
            merge_k: 16,
            canceled: AtomicBool::new(false),
        }
    }
}

impl SortConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn max_chunk_bytes(self, max_chunk_bytes: usize) -> Self {
        assert!(
            max_chunk_bytes > 0,
            "max_chunk_bytes must be greater than 0"
        );
        Self {
            max_chunk_bytes,
            ..self
        }
    }

    pub fn concurrency(self, concurrency: usize) -> Self {
        assert!(concurrency > 0, "concurrency must be greater than 0");
        Self {
            concurrency,
            ..self
        }
    }

    pub fn merge_k(self, merge_k: usize) -> Self {
        assert!(merge_k >= 2, "merge_k must not be less than 2");
        Self { merge_k, ..self }
    }
}

pub fn sort<K>(
    source: impl Iterator<Item = (K, Vec<u8>)> + Send,
    config: SortConfig,
) -> SortedIter<K>
where
    K: Ord + Pod + Copy + Send + Sync + std::fmt::Debug,
{
    let (output_tx, output_rx) = bounded(config.concurrency * 16);
    let chunk_dir = match FileChunkDir::<K>::new() {
        Ok(chunk_dir) => Arc::new(chunk_dir),
        Err(e) => {
            let _ = output_tx.send(Err(e));
            return SortedIter::new(output_rx, None);
        }
    };
    let (file_chunk_tx, file_chunk_rx) = unbounded();
    let chunk_dir = chunk_dir.clone();

    {
        let chunk_dir = chunk_dir.clone();
        rayon::ThreadPoolBuilder::new()
            .num_threads(config.concurrency + 1)
            .build()
            .unwrap()
            .install(|| {
                start_sorting_stage(&config, source, chunk_dir.clone(), file_chunk_tx);
                start_merging_stage(&config, file_chunk_rx, chunk_dir.clone(), output_tx);
            });
    }

    SortedIter::new(output_rx, Some(chunk_dir))
}

pub struct SortedIter<K: Pod> {
    output_rx: Receiver<Result<(K, Vec<u8>)>>,
    done: bool,
    #[allow(dead_code)]
    chunk_dir: Option<Arc<FileChunkDir<K>>>,
}

impl<K: Pod> SortedIter<K> {
    fn new(
        output_rx: Receiver<Result<(K, Vec<u8>)>>,
        chunk_dir: Option<Arc<FileChunkDir<K>>>,
    ) -> Self {
        SortedIter {
            output_rx,
            chunk_dir,
            done: false,
        }
    }
}

impl<K: Pod> Iterator for SortedIter<K> {
    type Item = Result<(K, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.output_rx.recv() {
            Ok(Ok(v)) => Some(Ok(v)),
            Ok(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            Err(_) => {
                self.done = true;
                None
            }
        }
    }
}

fn start_sorting_stage<K>(
    config: &SortConfig,
    source: impl Iterator<Item = (K, Vec<u8>)> + Send,
    chunk_dir: Arc<FileChunkDir<K>>,
    chunk_tx: Sender<Result<FileChunk<K>>>,
) where
    K: Ord + Pod + Copy + Send + Sync,
{
    debug!("Sorting stage started.");

    let item_header_size = std::mem::size_of::<Vec<u8>>();
    let mut chunk_size = 0;

    let mut buffer = Vec::new();

    fn mem_to_file_chunk<K: Pod + Ord>(
        buffer: Vec<(K, Vec<u8>)>,
        chunk_dir: Arc<FileChunkDir<K>>,
    ) -> Result<FileChunk<K>> {
        let mem_chunk = MemChunk::from_unsorted(buffer);
        let mut file_chunk = chunk_dir.add_chunk()?;
        mem_chunk.write_to_file(&mut file_chunk)?;
        Ok(file_chunk.finalize())
    }

    for (key, value) in source {
        let item_size = item_header_size + value.len();
        if chunk_size + item_size >= config.max_chunk_bytes {
            let buffer = std::mem::take(&mut buffer);
            let chunk_dir = chunk_dir.clone();
            let chunk_tx = chunk_tx.clone();
            rayon::spawn(move || {
                let _ = chunk_tx.send(mem_to_file_chunk(buffer, chunk_dir));
            });
            chunk_size = 0;
        }
        chunk_size += item_size;
        buffer.push((key, value));
    }

    // last chunk
    if !buffer.is_empty() {
        rayon::spawn(move || {
            let _ = chunk_tx.send(mem_to_file_chunk(buffer, chunk_dir));
        });
    }
}

fn start_merging_stage<K>(
    config: &SortConfig,
    chunk_rx: Receiver<Result<FileChunk<K>>>,
    chunk_dir: Arc<FileChunkDir<K>>,
    output_tx: Sender<Result<(K, Vec<u8>)>>,
) where
    K: Ord + Pod + Copy + Send + Sync + std::fmt::Debug,
{
    debug!("Merging stage started.");

    let (merged_tx, merged_rx) = unbounded::<Result<FileChunk<K>>>();
    let mut pending = Vec::new();
    let mut source_finished = false;
    let mut num_running_merges = 0;

    let mut recv_select = Select::new();
    recv_select.recv(&chunk_rx); // 0
    recv_select.recv(&merged_rx); // 1

    loop {
        let idx = recv_select.ready();
        match idx {
            // Receive chunks from the sorting stage
            0 => match chunk_rx.try_recv() {
                Ok(Ok(chunk)) => {
                    debug!("Received chunk: items={}", chunk.len());
                    pending.push(chunk)
                }
                Ok(Err(e)) => {
                    let _ = output_tx.send(Err(e));
                    break;
                }
                Err(_) => {
                    debug!("All chunks received from the sorting stage");
                    source_finished = true;
                    recv_select.remove(0);
                }
            },
            // Receive merged chunks
            1 => match merged_rx.try_recv() {
                Ok(Ok(chunk)) => {
                    debug!("Received merged chunk: items={}", chunk.len());
                    num_running_merges -= 1;
                    pending.push(chunk)
                }
                Ok(Err(e)) => {
                    let _ = output_tx.send(Err(e));
                    break;
                }
                Err(_) => {
                    panic!("merged_rx should not be closed at this point")
                }
            },
            _ => unreachable!(),
        }

        // Plan to merge
        let total_chunks = pending.len() + num_running_merges;
        let num_merge = if source_finished {
            if pending.len() > config.merge_k {
                (total_chunks - config.merge_k + 1).min(config.merge_k)
            } else if num_running_merges == 0 {
                break;
            } else {
                continue;
            }
        } else if total_chunks >= config.merge_k * 2 - 1 {
            if pending.len() >= config.merge_k {
                config.merge_k
            } else {
                continue;
            }
        } else {
            continue;
        };

        pending.sort_by_key(|chunk| chunk.len());
        let remaining = pending.split_off(num_merge.min(pending.len()));
        let merging = std::mem::replace(&mut pending, remaining);

        let merged_tx = merged_tx.clone();
        let mut chunk_writer = match chunk_dir.add_chunk() {
            Ok(chunk_writer) => chunk_writer,
            Err(e) => {
                let _ = output_tx.send(Err(e));
                break;
            }
        };

        // Start merging
        debug!("Start merging {} chunks", merging.len());
        num_running_merges += 1;
        rayon::spawn(move || {
            match merge_chunks_with_binary_heap(merging, |(key, value)| {
                chunk_writer.push(&key, &value)
            }) {
                Ok(()) => {
                    let _ = merged_tx.send(Ok(chunk_writer.finalize()));
                }
                Err(e) => {
                    let _ = merged_tx.send(Err(e));
                }
            }
        });
    }

    debug!("Start iteration by merging {} chunks", pending.len());
    rayon::spawn(move || {
        if let Err(e) = merge_chunks_with_binary_heap(pending, |(key, value)| {
            let _ = output_tx.send(Ok((key, value)));
            Ok(())
        }) {
            let _ = merged_tx.send(Err(e));
        }
        drop(chunk_dir);
    });
}

#[allow(dead_code)]
fn merge_chunks_by_naive_picking<K>(
    chunks: Vec<FileChunk<K>>,
    mut add_fn: impl FnMut((K, Vec<u8>)) -> Result<()>,
) -> Result<()>
where
    K: Ord + Pod + Copy + Send + Sync,
{
    let tmp_file_paths = chunks
        .iter()
        .map(|chunk| chunk.path().to_owned())
        .collect::<Vec<_>>();

    let mut chunk_iters = chunks
        .into_iter()
        .map(|chunk| Ok(chunk.iter()?.peekable()))
        .collect::<Result<Vec<_>>>()?;

    loop {
        let mut min_key = None;
        let mut min_key_idx = None;
        let mut found_ranout = false;

        for (idx, iter) in chunk_iters.iter_mut().enumerate() {
            match iter.peek() {
                Some(Ok((key, _))) => {
                    if min_key.is_none()
                        || key < min_key.as_ref().expect("min_key should have value")
                    {
                        min_key = Some(*key);
                        min_key_idx = Some(idx);
                    }
                }
                Some(Err(_)) => {
                    min_key_idx = Some(idx);
                    break;
                }
                None => {
                    found_ranout = true;
                }
            }
        }

        if let Some(min_key_idx) = min_key_idx {
            match chunk_iters[min_key_idx].next() {
                Some(Ok((key, value))) => {
                    add_fn((key, value))?;
                }
                Some(Err(e)) => {
                    return Err(e);
                }
                None => unreachable!(),
            }
        } else {
            break;
        }

        if found_ranout {
            // remove ran-out iterators
            chunk_iters.retain_mut(|it| it.peek().is_some());
        }
    }

    for path in tmp_file_paths {
        if std::fs::remove_file(&path).is_err() {
            warn!("Failed to remove file: {:?}", path);
        }
    }

    Ok(())
}
