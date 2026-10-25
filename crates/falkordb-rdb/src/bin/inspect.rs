use std::path::PathBuf;

fn main() {
    if let Err(error) = run() {
        eprintln!("falkordb-rdb-inspect: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: falkordb-rdb-inspect DUMP")?;
    let bytes = std::fs::read(&path)?;
    let graph = falkordb_rdb::decode_dump(&bytes)?;
    println!("{}", serde_json::to_string_pretty(&graph)?);
    Ok(())
}
