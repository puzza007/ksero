extern crate clap;
use clap::App;

fn main() {
    let matches = App::new("Ksero")
        .version("0.1.0")
        .author("Paul Oliver <puzza007@gmail.com>")
        .about("Duplicate File Finder")
        .args_from_usage("--directories=<DIRECTORY>... 'Sets directories to search'")
        .get_matches();

    let directories: Vec<_> = matches.values_of("directories").unwrap().collect();
    println!("Value for directory: {:?}", directories);
}
