//! Dependency-direction guard for the Application Core.

#[test]
fn application_normal_graph_has_no_infrastructure_adapters() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("workspace root");
    let output = std::process::Command::new("cargo")
        .args([
            "tree",
            "--manifest-path",
            root.join("Cargo.toml").to_str().unwrap(),
            "-p",
            "rstore-application",
            "--edges",
            "normal",
        ])
        .output()
        .expect("cargo metadata is available for architecture tests");
    assert!(output.status.success(), "cargo tree failed");
    let tree = String::from_utf8(output.stdout).unwrap();
    for forbidden in ["rstore-persistence", "sqlx", "aws-sdk", "gpui"] {
        assert!(
            !tree.contains(forbidden),
            "Application normal dependency graph contains forbidden edge `{forbidden}`\n{tree}"
        );
    }
}
