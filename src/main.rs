extern crate clap;

use clap::App;
use walkdir::{DirEntry, WalkDir};
use std::fs::File;
use std::io::Read;
use std::error::Error;
use std::collections::HashMap;
use rayon::prelude::*;
use twox_hash;
use std::hash::Hasher;

fn is_empty(entry: &DirEntry) -> bool {
    entry.metadata().map(|m| m.len() == 0).unwrap_or(false)
}


fn main() {
    let mut files_by_size = HashMap::new();

    let matches = App::new("Ksero")
        .version("0.1.0")
        .author("Paul Oliver <puzza007@gmail.com>")
        .about("Duplicate File Finder")
        .args_from_usage("--directories=<DIRECTORY>... 'Sets directories to search'")
        .get_matches();

    // Find files of duplicate size
    if let Some(directories) = matches.values_of("directories") {
        for d in directories.into_iter() {
            for entry in WalkDir::new(d)
                .follow_links(true)
                .into_iter()
                .filter_entry(|e| !is_empty(e))
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let file_size = entry.metadata().unwrap().len();
                    files_by_size.entry(file_size).or_insert_with(Vec::new).push(entry);
                }
            }
        }
    }

    // Find file with duplicate hash of first N bytes
    let mut files_by_hash_chunk = HashMap::new();

    let mut files_by_hash_chunk_work = Vec::with_capacity(50000);

    for (_k, v) in files_by_size.iter() {
        if v.len() > 1 {
            for entry in v.iter() {
                files_by_hash_chunk_work.push(entry);
            }
        }
    }

    let results: Vec<(u64, String)> = files_by_hash_chunk_work.par_iter().map(|entry| {
        let mut digest = twox_hash::XxHash::with_seed(0);
        let mut f = match File::open(entry.path()) {
            Ok(f) => f,
            Err(e) => {
                panic!("Failed to open file: {}", e.description());
            }
        };
        const CHUNK_SIZE: usize = 1024;
        let mut buffer = [0; CHUNK_SIZE];

        match f.read(&mut buffer) {
            Err(e) => panic!("Failed to read file: {:?}", e.description()),
            Ok(n) => digest.write(&buffer[0..n])
        }

        let digest_sum = digest.finish();

        (digest_sum, entry.path().to_str().unwrap().to_string())
    }).collect();

    for (digest_sum, path) in results.iter() {
        files_by_hash_chunk.entry(digest_sum).or_insert_with(Vec::new).push(path);
    }

    let mut files_by_hash = HashMap::new();

    let mut files_by_hash_work = Vec::with_capacity(50000);

    // Now go the whole hog and checksum the entire file
    for (_k, v) in files_by_hash_chunk.iter() {
        if v.len() > 1 {
            for path in v.iter() {
                files_by_hash_work.push(path);
            }
        }
    }

    let final_results: Vec<(u64, String)> = files_by_hash_work.par_iter().map(|path| {
        let mut digest = twox_hash::XxHash::with_seed(0);
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                panic!("Failed to open file: {}", e.description());
            }
        };
        const CHUNK_SIZE: usize = 1024 * 128;
        let mut buffer = [0; CHUNK_SIZE];

        // iterate
        loop {
            match f.read(&mut buffer) {
                Err(e) => panic!("Failed to read file: {:?}", e.description()),
                Ok(0) => break,
                Ok(n) => digest.write(&buffer[0..n])
            }
        }

        let digest_sum = digest.finish();

        (digest_sum, path.to_string())
    }).collect();

    for (digest_sum, path) in final_results.iter() {
        files_by_hash.entry(digest_sum).or_insert_with(Vec::new).push(path);
    }

    for (k, v) in files_by_hash.iter() {
        if v.len() > 1 {
            println!("{}: {:?}", k, v);
        }
    }
}
