extern crate clap;

use clap::App;
use pretty_bytes::converter;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::hash::Hasher;
use std::io;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use walkdir::{DirEntry, WalkDir};

struct HashWriter<T: Hasher>(T);

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
struct FileDetails {
    digest: u64,
    size: u64,
}

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
    let mut bytes_considered = 0;

    // Find files of duplicate size
    if let Some(directories) = matches.values_of("directories") {
        for d in directories {
            for entry in WalkDir::new(d)
                .into_iter()
                .filter_entry(|e| !is_empty(e))
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let file_size = entry.metadata().unwrap().len();
                    files_considered += 1;
                    bytes_considered += file_size;
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

    let results: Vec<(FileDetails, String)> = files_by_hash_chunk_work
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
                            let bytes = f.metadata().unwrap().len();
                            let file_details = FileDetails {
                                digest: digest_sum,
                                size: bytes,
                            };
                            Some((file_details, entry.path().to_str().unwrap().to_string()))
                        }
                    }
                }
            }
        })
        .collect();

    for (file_details, path) in results.iter() {
        files_by_hash_chunk
            .entry(file_details)
            .or_insert_with(Vec::new)
            .push(path);
    }

    let mut files_by_hash = HashMap::with_capacity(10000);

    let mut files_by_hash_work = Vec::with_capacity(50000);

    let mut files_nibbled = 0;
    let mut bytes_nibbled = 0;

    // Now go the whole hog and checksum the entire file
    for (file_details, v) in files_by_hash_chunk.iter() {
        files_nibbled += v.len();
        bytes_nibbled += file_details.size * v.len() as u64;
        if v.len() > 1 {
            for path in v.iter() {
                files_by_hash_work.push(path);
            }
        }
    }

    let mut files_hashed = 0;
    let mut bytes_hashed = 0;

    let final_results: Vec<(FileDetails, String)> = files_by_hash_work
        .par_iter()
        .filter_map(|path| {
            let mut digest_writer = HashWriter(twox_hash::XxHash::with_seed(0));
            match File::open(path) {
                Err(_) => None,
                Ok(mut f) => match std::io::copy(&mut f, &mut digest_writer) {
                    Ok(_) => {
                        let digest_sum = digest_writer.finish();
                        let bytes = f.metadata().unwrap().len();
                        let file_details = FileDetails {
                            digest: digest_sum,
                            size: bytes,
                        };
                        Some((file_details, path.to_string()))
                    }
                    Err(_) => None,
                },
            }
        })
        .collect();

    for (file_details, path) in final_results.iter() {
        files_by_hash
            .entry(file_details)
            .or_insert_with(Vec::new)
            .push(path);
    }

    let duration = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        - start.duration_since(UNIX_EPOCH).unwrap().as_millis()) as f64
        / 1000.0;

    for (file_details, v) in files_by_hash.iter() {
        let num_files = v.len();
        files_hashed += num_files;
        let duplicates_size = file_details.size * num_files as u64;
        bytes_hashed += duplicates_size;
        if v.len() > 1 {
            println!(
                "{} {} {}",
                file_details.digest,
                num_files,
                converter::convert(duplicates_size as f64)
            );
            for path in v {
                println!("\t\"{}\"", path);
            }
        }
    }

    let files_skipped_due_to_size = files_considered - files_nibbled;
    let bytes_skipped_due_to_size = bytes_considered - bytes_nibbled;

    eprintln!("Time                       : {:.4} seconds", duration);
    eprintln!(
        "Considered                 : {} files {}",
        files_considered,
        converter::convert(bytes_considered as f64)
    );
    eprintln!(
        "Skipped due to unique size : {} files {}",
        files_skipped_due_to_size,
        converter::convert(bytes_skipped_due_to_size as f64)
    );
    eprintln!(
        "Nibbled                    : {} files {}",
        files_nibbled,
        converter::convert(bytes_nibbled as f64)
    );
    eprintln!(
        "Hashed                     : {} files {}",
        files_hashed,
        converter::convert(bytes_hashed as f64)
    );
}
