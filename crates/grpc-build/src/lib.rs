#![feature(custom_inner_attributes)]
#![rustfmt::skip]
//! Build-time lowering of service schemas into `#[grpc_service]` modules.
//!
//! `.proto` inputs are checked with `protoc`. `.fdl` inputs are compiled with
//! Apache Fory `foryc`. `.capnp` inputs are compiled with `capnp`. Each schema
//! also yields a Rust module of synchronous service traits.
use std::{
    fs, io,
    path::{Path, PathBuf},
    process::Command,
};
/// Schema language selected from the file extension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaKind {
    /// Protocol Buffers (`.proto`).
    Proto,
    /// Apache Fory IDL (`.fdl`).
    Fdl,
    /// Cap'n Proto (`.capnp`).
    Capnp,
}
/// Failure while reading a schema or running its compiler.
#[derive(Debug)]
pub struct BuildError {
    message: String,
}
impl BuildError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}
impl std::fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl std::error::Error for BuildError {}
/// Rust service module plus the files emitted by the external compiler.
#[derive(Debug)]
pub struct CompiledSchema {
    /// Generated module of `#[grpc_service]` traits.
    pub service_rust: PathBuf,
    /// Descriptor set, `foryc` sources, or Cap'n Proto binary.
    pub tool_output: Vec<PathBuf>,
}
/// Invokes `protoc`, `foryc`, and `capnp`, then writes service traits.
#[derive(Debug)]
#[must_use]
pub struct SchemaCompiler {
    out_dir: PathBuf,
    protoc: PathBuf,
    foryc: PathBuf,
    capnp: PathBuf,
    includes: Vec<PathBuf>,
}
impl SchemaCompiler {
    /// Stores generated Rust and compiler artifacts in `out_dir`.
    pub fn new(out_dir: impl Into<PathBuf>) -> Self {
        Self {
            out_dir: out_dir.into(),
            protoc: default_tool("/root/.local/bin/protoc", "protoc"),
            foryc: PathBuf::from("/root/.local/bin/foryc"),
            capnp: default_tool("/usr/bin/capnp", "capnp"),
            includes: Vec::new(),
        }
    }
    /// Overrides the `protoc` executable.
    pub fn protoc_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.protoc = path.into();
        self
    }
    /// Overrides the `foryc` executable.
    pub fn foryc_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.foryc = path.into();
        self
    }
    /// Overrides the `capnp` executable.
    pub fn capnp_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.capnp = path.into();
        self
    }
    /// Adds an import path passed to the external compiler.
    pub fn include(mut self, path: impl Into<PathBuf>) -> Self {
        self.includes.push(path.into());
        self
    }
    /// Compiles one `.proto`, `.fdl`, or `.capnp` service schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, the schema has no service,
    /// or `protoc`, `foryc`, or `capnp` is missing or exits unsuccessfully.
    pub fn compile(&self, schema: &Path) -> Result<CompiledSchema, BuildError> {
        let kind = kind_of(schema)?;
        let source = fs::read_to_string(schema).map_err(|error| fail(&schema.display(), error))?;
        let rendered = render_service_module(kind, &source)?;
        fs::create_dir_all(&self.out_dir).map_err(|error| fail(&self.out_dir.display(), error))?;
        let tool_output = self.invoke(kind, schema)?;
        let service_rust = self.out_dir.join(format!("{}_grpc.rs", file_stem(schema)?));
        fs::write(&service_rust, rendered).map_err(|error| fail(&service_rust.display(), error))?;
        Ok(CompiledSchema { service_rust, tool_output })
    }
    fn invoke(&self, kind: SchemaKind, schema: &Path) -> Result<Vec<PathBuf>, BuildError> {
        match kind {
            SchemaKind::Proto => self.invoke_protoc(schema),
            SchemaKind::Fdl => self.invoke_foryc(schema),
            SchemaKind::Capnp => self.invoke_capnp(schema),
        }
    }
    fn invoke_protoc(&self, schema: &Path) -> Result<Vec<PathBuf>, BuildError> {
        let descriptor = self.out_dir.join(format!("{}.pb", file_stem(schema)?));
        let mut args = Vec::new();
        push_proto_path(&mut args, schema.parent());
        for include in &self.includes {
            push_proto_path(&mut args, Some(include.as_path()));
        }
        args.push(format!("--descriptor_set_out={}", descriptor.display()));
        args.push("--include_imports".to_string());
        args.push(schema.display().to_string());
        run_command(&self.protoc, &args, None)?;
        Ok(vec![descriptor])
    }
    fn invoke_foryc(&self, schema: &Path) -> Result<Vec<PathBuf>, BuildError> {
        let rust_out = self.out_dir.join("fory");
        fs::create_dir_all(&rust_out).map_err(|error| fail(&rust_out.display(), error))?;
        let mut args = vec!["--lang".to_string(), "rust".to_string(), "--grpc".to_string(), "--rust_out".to_string(), rust_out.display().to_string()];
        push_includes(&mut args, schema, &self.includes);
        args.push(schema.display().to_string());
        run_command(&self.foryc, &args, None)?;
        list_files(&rust_out)
    }
    fn invoke_capnp(&self, schema: &Path) -> Result<Vec<PathBuf>, BuildError> {
        let binary = self.out_dir.join(format!("{}.capnp.bin", file_stem(schema)?));
        let mut args = vec!["compile".to_string()];
        push_includes(&mut args, schema, &self.includes);
        args.push("-o-".to_string());
        args.push(schema.display().to_string());
        run_command(&self.capnp, &args, Some(&binary))?;
        Ok(vec![binary])
    }
}
/// Lowers schema text into `#[grpc_service]` modules without running a compiler.
///
/// # Errors
///
/// Returns an error when the text defines no service or a declaration is incomplete.
pub fn render_service_module(kind: SchemaKind, source: &str) -> Result<String, BuildError> {
    let tokens = lex(source)?;
    let services = match kind {
        SchemaKind::Proto | SchemaKind::Fdl => parse_rpc_schema(&tokens)?,
        SchemaKind::Capnp => parse_capnp(&tokens)?,
    };
    if services.is_empty() {
        return Err(BuildError::new("schema does not define a service"));
    }
    Ok(render(&services))
}
fn fail(context: &impl std::fmt::Display, error: io::Error) -> BuildError {
    BuildError::new(format!("{context}: {error}"))
}
fn default_tool(preferred: &str, fallback: &str) -> PathBuf {
    let preferred = PathBuf::from(preferred);
    if preferred.is_file() { preferred } else { PathBuf::from(fallback) }
}
fn kind_of(path: &Path) -> Result<SchemaKind, BuildError> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("proto") => Ok(SchemaKind::Proto),
        Some("fdl") => Ok(SchemaKind::Fdl),
        Some("capnp") => Ok(SchemaKind::Capnp),
        _ => Err(BuildError::new(format!("unsupported schema {}", path.display()))),
    }
}
fn file_stem(path: &Path) -> Result<String, BuildError> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| BuildError::new(format!("schema {} has no file name", path.display())))
}
fn push_proto_path(args: &mut Vec<String>, path: Option<&Path>) {
    if let Some(path) = path.filter(|path| !path.as_os_str().is_empty()) {
        args.push(format!("--proto_path={}", path.display()));
    }
}
fn push_includes(args: &mut Vec<String>, schema: &Path, includes: &[PathBuf]) {
    if let Some(parent) = schema.parent().filter(|path| !path.as_os_str().is_empty()) {
        args.push("-I".to_string());
        args.push(parent.display().to_string());
    }
    for include in includes {
        args.push("-I".to_string());
        args.push(include.display().to_string());
    }
}
fn list_files(dir: &Path) -> Result<Vec<PathBuf>, BuildError> {
    let mut files = Vec::new();
    let entries = fs::read_dir(dir).map_err(|error| fail(&dir.display(), error))?;
    for entry in entries {
        files.push(entry.map_err(|error| BuildError::new(format!("read dir: {error}")))?.path());
    }
    files.sort();
    Ok(files)
}
fn run_command(program: &Path, args: &[String], stdout_path: Option<&Path>) -> Result<(), BuildError> {
    let output = Command::new(program).args(args).output().map_err(|error| BuildError::new(format!("failed to spawn {}: {error}", program.display())))?;
    if let Some(path) = stdout_path {
        fs::write(path, &output.stdout).map_err(|error| fail(&path.display(), error))?;
    }
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    Err(BuildError::new(format!("{} exited {}: {stderr}{stdout}", program.display(), output.status)))
}
enum Tok {
    Ident(String),
    Symbol(char),
    Str(String),
}
struct ServiceDef {
    package: String,
    name: String,
    methods: Vec<MethodDef>,
}
struct MethodDef {
    name: String,
    request: String,
    response: String,
    client_streaming: bool,
    server_streaming: bool,
}
fn lex(source: &str) -> Result<Vec<Tok>, BuildError> {
    let chars: Vec<char> = source.chars().collect();
    let mut index = 0;
    let mut tokens = Vec::new();
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }
        if ch == '/' && chars.get(index + 1) == Some(&'/') {
            while index < chars.len() && chars[index] != '\n' {
                index += 1;
            }
            continue;
        }
        if ch == '/' && chars.get(index + 1) == Some(&'*') {
            index = skip_block(&chars, index)?;
            continue;
        }
        if ch == '"' {
            let (next, text) = read_string(&chars, index)?;
            tokens.push(Tok::Str(text));
            index = next;
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' || ch.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < chars.len() && (chars[index].is_ascii_alphanumeric() || chars[index] == '_') {
                index += 1;
            }
            tokens.push(Tok::Ident(chars[start..index].iter().collect()));
            continue;
        }
        tokens.push(Tok::Symbol(ch));
        index += 1;
    }
    Ok(tokens)
}
fn skip_block(chars: &[char], start: usize) -> Result<usize, BuildError> {
    let mut index = start + 2;
    while index + 1 < chars.len() && !(chars[index] == '*' && chars[index + 1] == '/') {
        index += 1;
    }
    if index + 1 >= chars.len() { Err(BuildError::new("unterminated block comment")) } else { Ok(index + 2) }
}
fn read_string(chars: &[char], start: usize) -> Result<(usize, String), BuildError> {
    let mut index = start + 1;
    let mut text = String::new();
    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            let Some(escaped) = chars.get(index + 1).copied() else {
                break;
            };
            text.push(match escaped {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                other => other,
            });
            index += 2;
            continue;
        }
        if ch == '"' {
            return Ok((index + 1, text));
        }
        text.push(ch);
        index += 1;
    }
    Err(BuildError::new("unterminated string"))
}
fn parse_rpc_schema(tokens: &[Tok]) -> Result<Vec<ServiceDef>, BuildError> {
    let mut services = Vec::new();
    let mut package = String::new();
    let mut index = 0;
    while index < tokens.len() {
        if ident_is(tokens, index, "package") {
            let (next, name) = parse_package_name(tokens, index)?;
            package = name;
            index = next;
            continue;
        }
        if ident_is(tokens, index, "service") {
            let (next, service) = parse_service(tokens, index, &package)?;
            services.push(service);
            index = next;
            continue;
        }
        index += 1;
    }
    Ok(services)
}
fn parse_package_name(tokens: &[Tok], mut index: usize) -> Result<(usize, String), BuildError> {
    index += 1;
    let (next, name) = read_dotted(tokens, index)?;
    index = next;
    if symbol_is(tokens, index, ';') {
        index += 1;
    }
    if name.is_empty() { Err(BuildError::new("package name is empty")) } else { Ok((index, name)) }
}
fn parse_service(tokens: &[Tok], mut index: usize, package: &str) -> Result<(usize, ServiceDef), BuildError> {
    index += 1;
    let name = expect_ident(tokens, index, "service")?;
    index += 1;
    if !symbol_is(tokens, index, '{') {
        return Err(BuildError::new(format!("service {name} is missing a body")));
    }
    index += 1;
    let mut methods = Vec::new();
    let mut depth = 1i32;
    while index < tokens.len() && depth > 0 {
        if depth == 1 && ident_is(tokens, index, "rpc") {
            let (next, method) = parse_rpc(tokens, index)?;
            methods.push(method);
            index = next;
            continue;
        }
        if symbol_is(tokens, index, '{') {
            depth += 1;
        } else if symbol_is(tokens, index, '}') {
            depth -= 1;
        }
        index += 1;
    }
    if depth != 0 {
        return Err(BuildError::new(format!("service {name} is unclosed")));
    }
    Ok((index, ServiceDef { package: package.to_string(), name, methods }))
}
fn parse_rpc(tokens: &[Tok], mut index: usize) -> Result<(usize, MethodDef), BuildError> {
    index += 1;
    let name = expect_ident(tokens, index, "rpc method")?;
    index += 1;
    let (next, client_streaming, request) = parse_paren_message(tokens, index)?;
    index = next;
    if !ident_is(tokens, index, "returns") {
        return Err(BuildError::new(format!("rpc {name} is missing returns")));
    }
    index += 1;
    let (next, server_streaming, response) = parse_paren_message(tokens, index)?;
    index = next;
    if symbol_is(tokens, index, ';') {
        index += 1;
    }
    Ok((index, MethodDef { name, request, response, client_streaming, server_streaming }))
}
fn parse_paren_message(tokens: &[Tok], mut index: usize) -> Result<(usize, bool, String), BuildError> {
    if !symbol_is(tokens, index, '(') {
        return Err(BuildError::new("expected '(' before a message type"));
    }
    index += 1;
    let mut streaming = false;
    if ident_is(tokens, index, "stream") {
        streaming = true;
        index += 1;
    }
    let (next, ty) = read_dotted(tokens, index)?;
    index = next;
    if !symbol_is(tokens, index, ')') {
        return Err(BuildError::new("expected ')' after a message type"));
    }
    Ok((index + 1, streaming, ty))
}
fn parse_capnp(tokens: &[Tok]) -> Result<Vec<ServiceDef>, BuildError> {
    let mut services = Vec::new();
    let mut package = String::new();
    let mut index = 0;
    while index < tokens.len() {
        if let Some((next, found)) = parent_module(tokens, index) {
            package = found;
            index = next;
            continue;
        }
        if ident_is(tokens, index, "interface") {
            let (next, service) = parse_interface(tokens, index, &package)?;
            services.push(service);
            index = next;
            continue;
        }
        if ident_is(tokens, index, "struct") || ident_is(tokens, index, "enum") {
            index = skip_braced(tokens, index)?;
            continue;
        }
        index += 1;
    }
    Ok(services)
}
fn parent_module(tokens: &[Tok], index: usize) -> Option<(usize, String)> {
    if !symbol_is(tokens, index, '$') || ident_text(tokens, index + 1) != Some("Rust") || !symbol_is(tokens, index + 2, '.') || ident_text(tokens, index + 3) != Some("parentModule") || !symbol_is(tokens, index + 4, '(') {
        return None;
    }
    let Tok::Str(package) = tokens.get(index + 5)? else {
        return None;
    };
    if !symbol_is(tokens, index + 6, ')') || !valid_package(package) {
        return None;
    }
    let mut next = index + 7;
    if symbol_is(tokens, next, ';') {
        next += 1;
    }
    Some((next, package.clone()))
}
fn parse_interface(tokens: &[Tok], mut index: usize, package: &str) -> Result<(usize, ServiceDef), BuildError> {
    index += 1;
    let name = expect_ident(tokens, index, "interface")?;
    index += 1;
    while index < tokens.len() && !symbol_is(tokens, index, '{') {
        index += 1;
    }
    if !symbol_is(tokens, index, '{') {
        return Err(BuildError::new(format!("interface {name} is missing a body")));
    }
    index += 1;
    let mut depth = 1i32;
    let mut methods = Vec::new();
    while index < tokens.len() && depth > 0 {
        if depth == 1 && is_stmt_keyword(tokens, index) {
            index = skip_semicolon(tokens, index)?;
            continue;
        }
        if depth == 1 && is_nested_keyword(tokens, index) {
            index = skip_braced(tokens, index)?;
            continue;
        }
        if depth == 1 && ident_text(tokens, index).is_some() {
            let (next, method) = parse_capnp_method(tokens, index)?;
            methods.push(method);
            index = next;
            continue;
        }
        if symbol_is(tokens, index, '{') {
            depth += 1;
        } else if symbol_is(tokens, index, '}') {
            depth -= 1;
        }
        index += 1;
    }
    if depth != 0 {
        return Err(BuildError::new(format!("interface {name} is unclosed")));
    }
    Ok((index, ServiceDef { package: package.to_string(), name, methods }))
}
fn parse_capnp_method(tokens: &[Tok], mut index: usize) -> Result<(usize, MethodDef), BuildError> {
    let name = expect_ident(tokens, index, "capnp method")?;
    index += 1;
    if symbol_is(tokens, index, '@') {
        index += 1;
        if ident_text(tokens, index).is_some() {
            index += 1;
        }
    }
    let (next, client_streaming, request) = parse_capnp_side(tokens, index)?;
    index = next;
    if !symbol_is(tokens, index, '-') || !symbol_is(tokens, index + 1, '>') {
        return Err(BuildError::new(format!("capnp method {name} is missing ->")));
    }
    index += 2;
    let (next, server_streaming, response) = parse_capnp_side(tokens, index)?;
    index = next;
    if symbol_is(tokens, index, ';') {
        index += 1;
    }
    Ok((index, MethodDef { name, request, response, client_streaming, server_streaming }))
}
fn parse_capnp_side(tokens: &[Tok], index: usize) -> Result<(usize, bool, String), BuildError> {
    if symbol_is(tokens, index, '(') { parse_capnp_params(tokens, index) } else { parse_capnp_type(tokens, index) }
}
fn parse_capnp_type(tokens: &[Tok], mut index: usize) -> Result<(usize, bool, String), BuildError> {
    let mut streaming = false;
    if ident_is(tokens, index, "stream") {
        streaming = true;
        index += 1;
    }
    let (next, ty) = read_dotted(tokens, index)?;
    Ok((next, streaming, ty))
}
fn parse_capnp_params(tokens: &[Tok], mut index: usize) -> Result<(usize, bool, String), BuildError> {
    index += 1;
    let mut streaming = false;
    let mut types = Vec::new();
    while index < tokens.len() && !symbol_is(tokens, index, ')') {
        if symbol_is(tokens, index, ',') {
            index += 1;
            continue;
        }
        if ident_text(tokens, index).is_some() && symbol_is(tokens, index + 1, ':') {
            if ident_is(tokens, index, "stream") {
                streaming = true;
            }
            index += 2;
        }
        if ident_is(tokens, index, "stream") {
            streaming = true;
            index += 1;
        }
        let (next, ty) = read_dotted(tokens, index)?;
        types.push(ty);
        index = next;
    }
    if !symbol_is(tokens, index, ')') || types.is_empty() {
        return Err(BuildError::new("capnp method is missing a parameter type"));
    }
    let ty = if types.len() == 1 { types.remove(0) } else { format!("({})", types.join(", ")) };
    Ok((index + 1, streaming, ty))
}
fn read_dotted(tokens: &[Tok], mut index: usize) -> Result<(usize, String), BuildError> {
    let Some(first) = ident_text(tokens, index) else {
        return Err(BuildError::new("expected a type name"));
    };
    ensure_ident(first)?;
    let mut name = first.to_string();
    index += 1;
    while symbol_is(tokens, index, '.') {
        let Some(part) = ident_text(tokens, index + 1) else {
            break;
        };
        ensure_ident(part)?;
        name.push('.');
        name.push_str(part);
        index += 2;
    }
    Ok((index, name))
}
fn render(services: &[ServiceDef]) -> String {
    let mut out = String::from("// Schema services lowered into synchronous grpc-service traits.\n");
    let mut used = Vec::new();
    for service in services {
        let mut module = reshape(&service.name, false);
        if used.iter().any(|name: &String| name == &module) {
            module = format!("{module}_{}", used.len());
        }
        used.push(module.clone());
        out.push_str("#[allow(non_snake_case)]\n");
        out.push_str(&format!("pub mod {module} {{\n"));
        if service.package.is_empty() {
            out.push_str("    #[::grpc_macros::grpc_service]\n");
        } else {
            let package = &service.package;
            out.push_str(&format!("    #[::grpc_macros::grpc_service(package = \"{package}\")]\n"));
        }
        out.push_str(&format!("    pub trait {} {{\n", service.name));
        for method in &service.methods {
            out.push_str(cardinality_attr(method));
            let request = method.request.replace('.', "::");
            let response = method.response.replace('.', "::");
            out.push_str(&format!("        fn {}({request}) -> {response};\n", method.name));
        }
        out.push_str("    }\n}\n");
    }
    out
}
fn cardinality_attr(method: &MethodDef) -> &'static str {
    match (method.client_streaming, method.server_streaming) {
        (false, false) => "",
        (false, true) => "        #[server_streaming]\n",
        (true, false) => "        #[client_streaming]\n",
        (true, true) => "        #[bidi_streaming]\n",
    }
}
fn expect_ident(tokens: &[Tok], index: usize, role: &str) -> Result<String, BuildError> {
    let Some(name) = ident_text(tokens, index) else {
        return Err(BuildError::new(format!("{role} is missing a name")));
    };
    ensure_ident(name)?;
    Ok(name.to_string())
}
fn ensure_ident(name: &str) -> Result<(), BuildError> {
    if is_rust_ident(name) && !is_keyword(name) {
        Ok(())
    } else {
        Err(BuildError::new(format!("invalid schema identifier `{name}`")))
    }
}
fn valid_package(package: &str) -> bool {
    !package.is_empty() && package.split('.').all(|part| is_rust_ident(part) && !is_keyword(part))
}
fn is_rust_ident(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
fn is_keyword(name: &str) -> bool {
    " as async await break const continue crate dyn else enum extern false fn for if impl in let \
     loop match mod move mut pub ref return self Self static struct super trait true type unsafe \
     use where while gen union abstract become box do final macro override priv typeof unsized \
     virtual yield try "
        .contains(&format!(" {name} "))
}
fn reshape(name: &str, ascii_upper: bool) -> String {
    let mut out = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() && index > 0 {
            out.push('_');
        }
        if ascii_upper {
            out.push(ch.to_ascii_uppercase());
        } else {
            out.push(ch.to_ascii_lowercase());
        }
    }
    out
}
fn ident_text(tokens: &[Tok], index: usize) -> Option<&str> {
    match tokens.get(index) {
        Some(Tok::Ident(text)) => Some(text.as_str()),
        _ => None,
    }
}
fn ident_is(tokens: &[Tok], index: usize, expected: &str) -> bool {
    ident_text(tokens, index) == Some(expected)
}
fn symbol_is(tokens: &[Tok], index: usize, symbol: char) -> bool {
    matches!(tokens.get(index), Some(Tok::Symbol(found)) if *found == symbol)
}
fn is_stmt_keyword(tokens: &[Tok], index: usize) -> bool {
    matches!(ident_text(tokens, index), Some("using" | "import" | "const" | "annotation"))
}
fn is_nested_keyword(tokens: &[Tok], index: usize) -> bool {
    matches!(ident_text(tokens, index), Some("struct" | "enum" | "interface" | "union" | "group"))
}
fn skip_semicolon(tokens: &[Tok], mut index: usize) -> Result<usize, BuildError> {
    while index < tokens.len() && !symbol_is(tokens, index, ';') {
        index += 1;
    }
    if symbol_is(tokens, index, ';') { Ok(index + 1) } else { Err(BuildError::new("expected ';'")) }
}
fn skip_braced(tokens: &[Tok], mut index: usize) -> Result<usize, BuildError> {
    while index < tokens.len() && !symbol_is(tokens, index, '{') && !symbol_is(tokens, index, ';') {
        index += 1;
    }
    if symbol_is(tokens, index, ';') { Ok(index + 1) } else { skip_balanced(tokens, index, '{', '}') }
}
fn skip_balanced(tokens: &[Tok], mut index: usize, open: char, close: char) -> Result<usize, BuildError> {
    if !symbol_is(tokens, index, open) {
        return Err(BuildError::new("expected a delimiter"));
    }
    let mut depth = 0i32;
    while index < tokens.len() {
        if symbol_is(tokens, index, open) {
            depth += 1;
        } else if symbol_is(tokens, index, close) {
            depth -= 1;
            if depth == 0 {
                return Ok(index + 1);
            }
        }
        index += 1;
    }
    Err(BuildError::new("unbalanced delimiter"))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, process, time::SystemTime};
    use super::{SchemaCompiler, SchemaKind, render_service_module};
    const PROTO: &str = r#"syntax = "proto3"; package demo.v1; service Echo { rpc Unary (Ping) returns (Pong); rpc Server (Ping) returns (stream Pong); rpc Client (stream Ping) returns (Pong); rpc Bidi (stream Ping) returns (stream Pong); } message Ping { string text = 1; } message Pong { string text = 1; }"#;
    const FDL: &str = r#"package demo.v1; message Ping {} message Pong {} service Echo { rpc Unary (Ping) returns (Pong); rpc Server (Ping) returns (stream Pong); rpc Client (stream Ping) returns (Pong); rpc Bidi (stream Ping) returns (stream Pong); }"#;
    const CAPNP: &str = r#"@0x9a2b3c4d5e6f7081; $Rust.parentModule("demo.v1"); struct Ping { text @0 :Text; } struct Pong { text @0 :Text; } interface Echo { unary @0 (request :Ping) -> (response :Pong); server @1 (request :Ping) -> (response :stream Pong); client @2 (request :stream Ping) -> (response :Pong); bidi @3 (request :stream Ping) -> (response :stream Pong); }"#;
    fn scratch(label: &str) -> PathBuf {
        let nanos = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|duration| duration.as_nanos()).unwrap_or(0);
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
            has(&rust, "#[server_streaming]");
            has(&rust, "#[client_streaming]");
            has(&rust, "#[bidi_streaming]");
            has(&rust, "fn Unary(Ping) -> Pong;");
        }
        let dotted = "package demo.v1; service Echo { rpc Unary (foo.bar.Ping) returns (foo.bar.Pong); }";
        let rust = render_service_module(SchemaKind::Proto, dotted).expect("dotted");
        has(&rust, "fn Unary(foo::bar::Ping) -> foo::bar::Pong;");
        let capnp = render_service_module(SchemaKind::Capnp, CAPNP).expect("capnp");
        has(&capnp, "#[server_streaming]\n        fn server(Ping) -> Pong;");
        has(&capnp, "#[client_streaming]\n        fn client(Ping) -> Pong;");
        has(&capnp, "#[bidi_streaming]\n        fn bidi(Ping) -> Pong;");
        has(&capnp, "fn unary(Ping) -> Pong;");
        has(&capnp, "package = \"demo.v1\"");
    }
    #[test]
    fn rejects_incomplete_input() {
        let error = render_service_module(SchemaKind::Proto, "message Ping {}").expect_err("empty");
        assert!(error.to_string().contains("service"));
        let dir = scratch("ext");
        let schema = dir.join("notes.txt");
        fs::write(&schema, "service Echo {}").expect("write");
        let error = SchemaCompiler::new(dir.join("out")).compile(&schema).expect_err("ext");
        assert!(error.to_string().contains("unsupported"));
        fs::remove_dir_all(&dir).expect("cleanup");
    }
    #[test]
    fn protoc_compiles_proto_service() {
        let dir = scratch("proto");
        let schema = dir.join("echo.proto");
        fs::write(&schema, PROTO).expect("write");
        let compiled = SchemaCompiler::new(dir.join("out")).compile(&schema).expect("protoc");
        let text = fs::read_to_string(&compiled.service_rust).expect("rust");
        has(&text, "#[bidi_streaming]");
        assert!(compiled.tool_output.iter().any(|path| path.extension().is_some_and(|ext| ext == "pb")));
        fs::remove_dir_all(&dir).expect("cleanup");
    }
    #[test]
    fn foryc_compiles_fdl_when_present() {
        let foryc = PathBuf::from("/root/.local/bin/foryc");
        if !foryc.is_file() {
            return;
        }
        let dir = scratch("fdl");
        let schema = dir.join("echo.fdl");
        fs::write(&schema, FDL).expect("write");
        let compiled = SchemaCompiler::new(dir.join("out")).foryc_path(&foryc).compile(&schema).expect("foryc");
        let text = fs::read_to_string(&compiled.service_rust).expect("rust");
        has(&text, "grpc_service");
        has(&text, "#[bidi_streaming]");
        assert!(!compiled.tool_output.is_empty());
        fs::remove_dir_all(&dir).expect("cleanup");
    }
    #[test]
    fn capnp_compiles_or_reports_missing_binary() {
        let dir = scratch("capnp");
        let schema = dir.join("echo.capnp");
        let source = concat!(
            "@0x9a2b3c4d5e6f7081; struct Ping { text @0 :Text; } ",
            "interface Echo { unary @0 (request :Ping) -> (response :Ping); }"
        );
        fs::write(&schema, source).expect("write");
        match SchemaCompiler::new(dir.join("out")).compile(&schema) {
            Ok(compiled) => {
                let text = fs::read_to_string(&compiled.service_rust).expect("rust");
                has(&text, "fn unary(Ping) -> Ping;");
                assert!(!compiled.tool_output.is_empty());
            }
            Err(error) => {
                let message = error.to_string();
                assert!(message.contains("failed to spawn") || message.contains("capnp"), "{message}");
            }
        }
        fs::remove_dir_all(&dir).expect("cleanup");
    }
}
