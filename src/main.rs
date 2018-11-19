extern crate clap;

use clap::App;
use walkdir::{DirEntry, WalkDir};

fn is_empty(entry: &DirEntry) -> bool {
    entry.metadata().map(|m| m.len() == 0).unwrap_or(false)
}

fn main() {
    let matches = App::new("Ksero")
        .version("0.1.0")
        .author("Paul Oliver <puzza007@gmail.com>")
        .about("Duplicate File Finder")
        .args_from_usage("--directories=<DIRECTORY>... 'Sets directories to search'")
        .get_matches();

    if let Some(directories) = matches.values_of("directories") {
        for d in directories.into_iter() {
            for entry in WalkDir::new(d)
                .into_iter()
                .filter_entry(|e| !is_empty(e))
                .filter_map(|e| e.ok())
            {
                println!("{}", entry.path().display());
            }
        }
    }
}
