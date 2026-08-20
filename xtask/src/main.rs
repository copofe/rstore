//! xtask entry point. Build/package/sign/release helpers live outside the
//! product crates.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("help") | None => print_help(),
        Some("check") => run("cargo", &["check", "--workspace"]),
        Some("test") => run("cargo", &["test", "--workspace"]),
        Some("fmt") => run("cargo", &["fmt", "--all"]),
        Some("clippy") => run("cargo", &["clippy", "--workspace", "--", "-D", "warnings"]),
        Some(other) => {
            eprintln!("unknown xtask command: {other}");
            print_help();
            std::process::exit(2);
        }
    }
}

fn print_help() {
    println!("xtask commands: help | check | test | fmt | clippy");
}

fn run(cmd: &str, args: &[&str]) {
    let status = std::process::Command::new(cmd)
        .args(args)
        .status()
        .expect("failed to invoke command");
    std::process::exit(status.code().unwrap_or(1));
}
