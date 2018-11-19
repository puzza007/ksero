extern crate clap;

use clap::App;
use walkdir::{DirEntry, WalkDir};
use crc::{crc32, Hasher32};
use std::fs::File;
use std::io::Read;
use std::error::Error;
use std::collections::HashMap;
use rayon::prelude::*;

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
                println!("{}: {}", entry.path().display(), entry.file_type().is_file());
                if entry.file_type().is_file() {
                    let file_size = entry.metadata().unwrap().len();
                    files_by_size.entry(file_size).or_insert_with(Vec::new).push(entry);
                }
            }
        }
    }

    // Find file with duplicate crc32 of first N bytes
    let mut files_by_crc32_chunk = HashMap::new();

    for (k, v) in files_by_size.iter().filter(|&(_k, v)| v.len() > 1) {
        println!("{}: {:?}", k, v);

        for entry in v.iter() {
            let mut digest = crc32::Digest::new(crc32::IEEE);
            let mut f = match File::open(entry.path()) {
                Ok(f) => f,
                Err(e) => {
                    panic!("Failed to open file: {}", e.description());
                }
            };
            const CHUNK_SIZE: usize = 1024 * 128;
            let mut buffer = [0; CHUNK_SIZE];

            match f.read(&mut buffer) {
                Err(e) => panic!("Failed to read file: {:?}", e.description()),
                Ok(n) => digest.write(&buffer[0..n])
            }

            let digest_sum = digest.sum32();
            println!("chunk: {:X}", digest_sum);

            files_by_crc32_chunk.entry(digest_sum).or_insert_with(Vec::new).push(entry);
        }
    }

    let mut files_by_crc32 = HashMap::new();

    // Now go the whole hog and checksum the entire file
    for (_k, v) in files_by_crc32_chunk.iter().filter(|&(_k, v)| v.len() > 1) {
        for entry in v.iter() {
            let mut digest = crc32::Digest::new(crc32::IEEE);
            let mut f = match File::open(entry.path()) {
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

            let digest_sum = digest.sum32();
            println!("full: {:X}", digest_sum);

            files_by_crc32.entry(digest_sum).or_insert_with(Vec::new).push(entry);
        }
    }

    for (k, v) in files_by_crc32.iter().filter(|&(_k, v)| v.len() > 1) {
        println!("{}: {:?}", k, v);
    }
}
