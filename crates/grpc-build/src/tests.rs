use std::{fs, path::PathBuf, process, time::SystemTime};

use super::{SchemaCompiler, SchemaKind, render_service_module};
const PROTO: &str = r#"syntax = "proto3"; package demo.v1; service Echo { rpc Unary (Ping) returns (Pong); rpc Server (Ping) returns (stream Pong); rpc Client (stream Ping) returns (Pong); rpc Bidi (stream Ping) returns (stream Pong); } message Ping { string text = 1; } message Pong { string text = 1; }"#;
const FDL: &str = r#"package demo.v1; message Ping {} message Pong {} service Echo { rpc Unary (Ping) returns (Pong); rpc Server (Ping) returns (stream Pong); rpc Client (stream Ping) returns (Pong); rpc Bidi (stream Ping) returns (stream Pong); }"#;
const CAPNP: &str = r#"@0x9a2b3c4d5e6f7081; $Rust.parentModule("demo.v1"); struct Ping { text @0 :Text; } struct Pong { text @0 :Text; } interface Echo { unary @0 (request :Ping) -> (response :Pong); server @1 (request :Ping) -> (response :stream Pong); client @2 (request :stream Ping) -> (response :Pong); bidi @3 (request :stream Ping) -> (response :stream Pong); }"#;
fn scratch(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("grpc-build-{label}-{}-{nanos}", process::id()));
    fs::create_dir_all(&dir).expect("temp dir");
    dir
}
fn has(text: &str, needle: &str) {
    assert!(text.contains(needle), "missing {needle} in {text}");
}
#[test]
fn lowers_proto_fdl_capnp_and_dotted_types() {
    for (kind, source) in [(SchemaKind::Proto, PROTO), (SchemaKind::Fdl, FDL)] {
        let rust = render_service_module(kind, source).expect("render");
        has(&rust, "package = \"demo.v1\"");
        has(
            &rust,
            "#[server_streaming]\n        fn Server(Ping) -> Pong;",
        );
        has(
            &rust,
            "#[client_streaming]\n        fn Client(Ping) -> Pong;",
        );
        has(&rust, "#[bidi_streaming]\n        fn Bidi(Ping) -> Pong;");
        has(&rust, "fn Unary(Ping) -> Pong;");
    }
    let dotted =
        "package demo.v1; service Echo { rpc Unary (foo.bar.Ping) returns (foo.bar.Pong); }";
    has(
        &render_service_module(SchemaKind::Proto, dotted).expect("dotted"),
        "fn Unary(foo::bar::Ping) -> foo::bar::Pong;",
    );
    let capnp = render_service_module(SchemaKind::Capnp, CAPNP).expect("capnp");
    has(
        &capnp,
        "#[server_streaming]\n        fn server(Ping) -> Pong;",
    );
    has(
        &capnp,
        "#[client_streaming]\n        fn client(Ping) -> Pong;",
    );
    has(&capnp, "#[bidi_streaming]\n        fn bidi(Ping) -> Pong;");
    has(&capnp, "fn unary(Ping) -> Pong;");
    has(&capnp, "package = \"demo.v1\"");
}
#[test]
fn covers_schema_edges() {
    let buried = "message Job { string service = 1; string package = 2; } service Echo { rpc \
                  Unary (Ping) returns (Pong); }";
    let rust = render_service_module(SchemaKind::Proto, buried).expect("buried");
    assert_eq!(rust.matches("pub trait").count(), 1, "{rust}");
    has(&rust, "fn Unary(Ping) -> Pong;");
    let lead = "service Echo { rpc Unary (.demo.v1.Ping) returns (stream .demo.v1.Pong); }";
    has(
        &render_service_module(SchemaKind::Proto, lead).expect("lead"),
        "#[server_streaming]\n        fn Unary(demo::v1::Ping) -> demo::v1::Pong;",
    );
    let named = render_service_module(
        SchemaKind::Proto,
        "service Echo { rpc Unary (stream) returns (Pong); }",
    )
    .expect("stream type");
    has(&named, "fn Unary(stream) -> Pong;");
    assert!(!named.contains("streaming"), "{named}");
    has(
        &render_service_module(
            SchemaKind::Proto,
            "service Type { rpc Unary (Ping) returns (Pong); }",
        )
        .expect("keyword"),
        "pub mod type_ {",
    );
    let open = render_service_module(SchemaKind::Proto, "message Ping {").expect_err("brace");
    assert!(open.to_string().contains("unclosed"), "{open}");
    let dup = render_service_module(
        SchemaKind::Proto,
        "service Echo { rpc A (Ping) returns (Pong); rpc A (Ping) returns (Pong); }",
    )
    .expect_err("dup");
    assert!(dup.to_string().contains("duplicate"), "{dup}");
    has(
        &render_service_module(
            SchemaKind::Fdl,
            "import 'api/service.fdl'; service Echo { rpc Unary (Ping) returns (Pong); }",
        )
        .expect("quotes"),
        "fn Unary(Ping) -> Pong;",
    );
    let cap = "# interface Old { foo @0 (x :Ping) -> (y :Pong); }\nstruct Box { interface Inner { \
               hidden @0 (q :Ping) -> (r :Pong); } }\ninterface Echo {\n# }\nunary @0 (stream \
               :Ping) -> (response :Pong);\nserver @1 (request @0 :stream Ping);\nbare @2 -> \
               stream;\nlisted @3 (items :List(foo.Entry)) -> ();\nunit @4 \
               ();\n}\n$Rust.parentModule(\"demo.v1\");\n";
    let rust = render_service_module(SchemaKind::Capnp, cap).expect("capnp edges");
    assert!(!rust.contains("fn foo("), "{rust}");
    has(&rust, "fn hidden(Ping) -> Pong;");
    has(
        &rust,
        "    pub trait Echo {\n        fn unary(Ping) -> Pong;\n",
    );
    has(&rust, "#[client_streaming]\n        fn server(Ping) -> ();");
    has(&rust, "#[server_streaming]\n        fn bare(()) -> ();");
    has(&rust, "fn listed(List<foo::Entry>) -> ();");
    has(&rust, "fn unit(()) -> ();");
    assert_eq!(rust.matches("package = \"demo.v1\"").count(), 2, "{rust}");
    let bad = render_service_module(
        SchemaKind::Capnp,
        "$Rust.parentModule(\"demo::v1\"); interface E { u @0 (r :Ping) -> (s :Pong); }",
    )
    .expect_err("pkg");
    assert!(bad.to_string().contains("parentModule"), "{bad}");
    let other = render_service_module(
        SchemaKind::Capnp,
        "$Rust.other(\"nope\"); interface Echo { unary @0 (r :Ping) -> (s :Pong); }",
    )
    .expect("other");
    assert!(!other.contains("package ="), "{other}");
}
#[test]
fn rejects_incomplete_input() {
    let error = render_service_module(SchemaKind::Proto, "message Ping {}").expect_err("empty");
    assert!(error.to_string().contains("service"));
    let dir = scratch("ext");
    let schema = dir.join("notes.txt");
    fs::write(&schema, "service Echo {}").expect("write");
    let error = SchemaCompiler::new(dir.join("out"))
        .compile(&schema)
        .expect_err("ext");
    assert!(error.to_string().contains("unsupported"));
    fs::remove_dir_all(dir).expect("cleanup");
}
#[test]
fn protoc_compiles_proto_service() {
    let dir = scratch("proto");
    let schema = dir.join("echo.proto");
    fs::write(&schema, PROTO).expect("write");
    let compiled = SchemaCompiler::new(dir.join("out"))
        .compile(&schema)
        .expect("protoc");
    let text = fs::read_to_string(&compiled.service_rust).expect("rust");
    has(
        &text,
        "#[server_streaming]\n        fn Server(Ping) -> Pong;",
    );
    assert!(
        compiled
            .tool_output
            .iter()
            .any(|path| path.extension().is_some_and(|ext| ext == "pb") && path.is_file())
    );
    fs::remove_dir_all(dir).expect("cleanup");
}
#[test]
fn foryc_compiles_fdl_when_present() {
    let configured = SchemaCompiler::new("unused").foryc;
    let foryc = super::resolve_tool(&configured, std::env::var_os("PATH").as_deref());
    let Some(foryc) = foryc else {
        assert!(
            std::env::var_os("CI").is_none()
                && std::env::var_os("GRPC_BUILD_REQUIRE_FORYC").is_none()
                && std::env::var_os("FORYC").is_none(),
            "required foryc compiler unavailable: {}",
            configured.display()
        );
        return;
    };
    let dir = scratch("fdl");
    let schema = dir.join("echo.fdl");
    fs::write(&schema, FDL).expect("write");
    let compiled = SchemaCompiler::new(dir.join("out"))
        .foryc_path(foryc)
        .compile(&schema)
        .expect("foryc");
    let text = fs::read_to_string(&compiled.service_rust).expect("rust");
    has(&text, "grpc_service");
    has(&text, "#[bidi_streaming]\n        fn Bidi(Ping) -> Pong;");
    assert!(compiled.tool_output.iter().any(|path| path.is_file()));
    fs::remove_dir_all(dir).expect("cleanup");
}
#[test]
fn capnp_compiles_or_reports_missing_binary() {
    let present = PathBuf::from("/usr/bin/capnp").is_file()
        || process::Command::new("capnp")
            .arg("--version")
            .output()
            .is_ok();
    let dir = scratch("capnp");
    let schema = dir.join("echo.capnp");
    fs::write(&schema, CAPNP).expect("write");
    let result = SchemaCompiler::new(dir.join("out")).compile(&schema);
    if present {
        let compiled = result.expect("capnp");
        let text = fs::read_to_string(&compiled.service_rust).expect("rust");
        has(&text, "fn unary(Ping) -> Pong;");
        has(&text, "#[bidi_streaming]\n        fn bidi(Ping) -> Pong;");
        assert!(compiled.tool_output.iter().all(|path| path.is_file()));
    } else {
        let message = result.expect_err("missing").to_string();
        assert!(message.contains("failed to spawn"), "{message}");
    }
    fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn tool_lookup_uses_supplied_path_and_rejects_missing_compilers() {
    let dir = scratch("tools");
    let tool = dir.join("controlled-foryc");
    fs::write(&tool, b"compiler fixture").expect("tool fixture");
    let path = std::env::join_paths([dir.as_path()]).expect("search path");
    assert_eq!(
        super::resolve_tool(PathBuf::from("controlled-foryc").as_path(), Some(&path)),
        Some(tool.clone())
    );
    assert_eq!(super::resolve_tool(&tool, None), Some(tool));
    assert!(super::resolve_tool(PathBuf::from("absent-foryc").as_path(), Some(&path)).is_none());
    fs::remove_dir_all(dir).expect("cleanup");
}
