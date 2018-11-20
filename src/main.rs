#![feature(duration_as_u128)]
extern crate clap;

use clap::App;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io::Read;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};
use twox_hash;
use walkdir::{DirEntry, WalkDir};

struct HashWriter<T: Hasher>(T);

impl<T: Hasher> HashWriter<T> {
    fn finish(&self) -> u64 {
        self.0.finish()
    }
}

impl<T: Hasher> io::Write for HashWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf);
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.write(buf).map(|_| ())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn is_empty(entry: &DirEntry) -> bool {
    entry.metadata().map(|m| m.len() == 0).unwrap_or(false)
}

fn main() {
    let start = SystemTime::now();

    let mut files_by_size = HashMap::with_capacity(50000);

    let matches = App::new("Ksero")
        .version("0.1.0")
        .author("Paul Oliver <puzza007@gmail.com>")
        .about("Duplicate File Finder")
        .args_from_usage("--directories=<DIRECTORY>... 'Sets directories to search'")
        .get_matches();

    let mut files_considered = 0;

    // Find files of duplicate size
    if let Some(directories) = matches.values_of("directories") {
        for d in directories.into_iter() {
            for entry in WalkDir::new(d)
                .into_iter()
                .filter_entry(|e| !is_empty(e))
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    files_considered += 1;
                    let file_size = entry.metadata().unwrap().len();
                    files_by_size
                        .entry(file_size)
                        .or_insert_with(Vec::new)
                        .push(entry);
                }
            }
        }
    }

    // Find file with duplicate hash of first N bytes
    let mut files_by_hash_chunk = HashMap::with_capacity(25000);

    let mut files_by_hash_chunk_work = Vec::with_capacity(50000);

    for (_k, v) in files_by_size.iter() {
        if v.len() > 1 {
            for entry in v.iter() {
                files_by_hash_chunk_work.push(entry);
            }
        }
    }

    let results: Vec<(u64, String)> = files_by_hash_chunk_work
        .par_iter()
        .filter_map(|entry| {
            let mut digest = twox_hash::XxHash::with_seed(0);
            match File::open(entry.path()) {
                Err(_) => None,
                Ok(mut f) => {
                    const CHUNK_SIZE: usize = 1024;
                    let mut buffer = [0; CHUNK_SIZE];

                    match f.read(&mut buffer) {
                        Err(_) => None,
                        Ok(n) => {
                            digest.write(&buffer[0..n]);
                            let digest_sum = digest.finish();

                            Some((digest_sum, entry.path().to_str().unwrap().to_string()))
                        }
                    }
                }
            }
        })
        .collect();

    for (digest_sum, path) in results.iter() {
        files_by_hash_chunk
            .entry(digest_sum)
            .or_insert_with(Vec::new)
            .push(path);
    }

    let mut files_by_hash = HashMap::with_capacity(10000);

    let mut files_by_hash_work = Vec::with_capacity(50000);

    let mut files_nibbled = 0;

    // Now go the whole hog and checksum the entire file
    for (_k, v) in files_by_hash_chunk.iter() {
        files_nibbled += v.len();
        if v.len() > 1 {
            for path in v.iter() {
                files_by_hash_work.push(path);
            }
        }
    }

    let mut files_hashed = 0;

    let final_results: Vec<(u64, String)> = files_by_hash_work
        .par_iter()
        .filter_map(|path| {
            let mut digest_writer = HashWriter(twox_hash::XxHash::with_seed(0));
            match File::open(path) {
                Err(_) => None,
                Ok(mut f) => {
                    match std::io::copy(&mut f, &mut digest_writer) {
                        Ok(_) => {
                            let digest_sum = digest_writer.finish();
                            Some((digest_sum, path.to_string()))
                        },
                        Err(_) =>
                            None
                    }
                }
            }
        })
        .collect();

    for (digest_sum, path) in final_results.iter() {
        files_by_hash
            .entry(digest_sum)
            .or_insert_with(Vec::new)
            .push(path);
    }

    let duration = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        - start.duration_since(UNIX_EPOCH).unwrap().as_millis())
        as f64 / 1000.0;

    for (k, v) in files_by_hash.iter() {
        files_hashed += v.len();
        if v.len() > 1 {
            println!("{} {}", k, v.len());
            for path in v {
                println!("\t\"{}\"", path);
            }
        }
    }

    let files_skipped_due_to_size = files_considered - files_nibbled;

    eprintln!("Time                       : {:.4} seconds", duration);
    eprintln!("Considered                 : {}", files_considered);
    eprintln!("Nibbled                    : {}", files_nibbled);
    eprintln!("Hashed                     : {}", files_hashed);
    eprintln!("Skipped due to unique size : {}", files_skipped_due_to_size);
}
