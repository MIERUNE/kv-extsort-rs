use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use std::sync::Arc;

use bytemuck::Pod;

use crate::{
    chunk::{FileChunk, FileChunkDir, MemChunk},
    Result,
};

pub struct ExternalSorter {}

impl Default for ExternalSorter {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalSorter {
    pub fn new() -> Self {
        ExternalSorter {}
    }

    pub fn sort<K>(&self, source: impl Iterator<Item = (K, Vec<u8>)> + Send) -> SortedIter<K>
    where
        K: Ord + Pod + Copy + Send + Sync + std::fmt::Debug,
    {
        let (output_tx, output_rx) = bounded(1024);
        let chunk_dir = match FileChunkDir::<K>::new() {
            Ok(chunk_dir) => Arc::new(chunk_dir),
            Err(e) => {
                let _ = output_tx.send(Err(e));
                return SortedIter { output_rx };
            }
        };
        let (file_chunk_tx, file_chunk_rx) = unbounded();
        let chunk_dir = chunk_dir.clone();

        // Stage 1: In-memory sort and write to disk
        {
            let chunk_dir = chunk_dir.clone();
            rayon::ThreadPoolBuilder::new()
                .build()
                .unwrap()
                .install(move || {
                    start_sorting_stage(source, chunk_dir, file_chunk_tx);
                });
        }

        // Stage 2: Merge file chunks
        {
            let chunk_dir = chunk_dir.clone();
            rayon::ThreadPoolBuilder::new()
                .build()
                .unwrap()
                .install(move || {
                    start_merging_stage(file_chunk_rx, chunk_dir, output_tx);
                });
        }

        SortedIter { output_rx }
    }
}

pub struct SortedIter<K> {
    output_rx: Receiver<Result<(K, Vec<u8>)>>,
}

impl<K> Iterator for SortedIter<K> {
    type Item = Result<(K, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.output_rx.recv().ok()
    }
}

fn start_sorting_stage<K>(
    source: impl Iterator<Item = (K, Vec<u8>)> + Send,
    chunk_dir: Arc<FileChunkDir<K>>,
    chunk_tx: Sender<FileChunk<K>>,
) where
    K: Ord + Pod + Copy + Send + Sync + std::fmt::Debug,
{
    let item_header_size = std::mem::size_of::<Vec<u8>>();
    let chunk_max_size = 100_000_000;
    let mut chunk_size = 0;
    let mut total_size = 0;

    let mut buffer = Vec::new();

    for (key, value) in source {
        let item_size = item_header_size + value.len();
        if chunk_size + item_size >= chunk_max_size {
            let buffer = std::mem::take(&mut buffer);
            let chunk_dir = chunk_dir.clone();
            let chunk_tx = chunk_tx.clone();
            rayon::spawn(move || {
                let mem_chunk = MemChunk::from_unsorted(buffer);
                let mut file_chunk = chunk_dir.add_chunk().unwrap();
                mem_chunk.write_to_file(&mut file_chunk).unwrap();
                let _ = chunk_tx.send(file_chunk.finalize());
            });
            total_size += chunk_size;
            chunk_size = 0;
        }
        chunk_size += item_size;
        buffer.push((key, value));
    }
    if !buffer.is_empty() {
        rayon::spawn(move || {
            let mem_chunk = MemChunk::from_unsorted(buffer);
            let mut file_chunk = chunk_dir.add_chunk().unwrap();
            mem_chunk.write_to_file(&mut file_chunk).unwrap();
            let _ = chunk_tx.send(file_chunk.finalize());
        });
        total_size += chunk_size;
    }

    println!("Total size: {}", total_size / 1000 / 1000);
}

fn start_merging_stage<K>(
    chunk_rx: Receiver<FileChunk<K>>,
    chunk_dir: Arc<FileChunkDir<K>>,
    output_tx: Sender<Result<(K, Vec<u8>)>>,
) where
    K: Ord + Pod + Copy + Send + Sync + std::fmt::Debug,
{
    let (merged_tx, merged_rx) = unbounded::<Result<FileChunk<K>>>();
    let mut pending = Vec::new();
    let mut source_finished = false;
    let mut merge_running = 0;
    let max_merge_size = 16;

    loop {
        select! {
            recv(chunk_rx) -> chunk => {
                match chunk {
                    Ok(chunk) => pending.push(chunk),
                    Err(_) => source_finished = true,
                }
            }
            recv(merged_rx) -> merge_result => {
                if let Ok(merge_result) = merge_result {
                    match merge_result {
                        Ok(chunk) => {
                            merge_running -= 1;
                            pending.push(chunk)
                        }
                        Err(e) => {
                            println!("{:?}", e);
                            break;
                        }
                    }
                }
            }
        }

        if pending.len() > max_merge_size {
            pending.sort_by_key(|chunk| chunk.len());
            let remaining = pending.split_off(max_merge_size);
            let merging = std::mem::replace(&mut pending, remaining);

            let merged_tx = merged_tx.clone();
            let mut chunk_writer = chunk_dir.add_chunk().unwrap();
            merge_running += 1;
            rayon::spawn(move || {
                match merge(merging, |(key, value)| chunk_writer.push(&key, &value)) {
                    Ok(()) => {
                        let _ = merged_tx.send(Ok(chunk_writer.finalize()));
                    }
                    Err(e) => {
                        let _ = merged_tx.send(Err(e));
                    }
                }
            });
        } else if source_finished && merge_running == 0 {
            break;
        }
    }

    rayon::spawn(move || {
        merge(pending, |(key, value)| {
            let _ = output_tx.send(Ok((key, value)));
            Ok(())
        })
        .unwrap();
        drop(chunk_dir);
    });
}

fn merge<K>(
    chunks: Vec<FileChunk<K>>,
    mut add_fn: impl FnMut((K, Vec<u8>)) -> Result<()>,
) -> Result<()>
where
    K: Ord + Pod + Copy + Send + Sync + std::fmt::Debug,
{
    let mut chunk_iters = chunks
        .into_iter()
        .map(|chunk| chunk.iter(1_000_000).unwrap().peekable())
        .collect::<Vec<_>>();

    loop {
        let mut min_key = None;
        let mut min_key_idx = None;
        let mut found_ranout = false;

        for (idx, iter) in chunk_iters.iter_mut().enumerate() {
            match iter.peek() {
                Some(Ok((key, _))) => {
                    if min_key.is_none() || key < min_key.as_ref().unwrap() {
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
                    add_fn((key, value)).unwrap();
                }
                Some(Err(e)) => {
                    return Err(e);
                }
                None => unreachable!(),
            }
        } else {
            return Ok(());
        }

        if found_ranout {
            // remove ran-out iterators
            chunk_iters.retain_mut(|it| it.peek().is_some());
        }
    }
}
