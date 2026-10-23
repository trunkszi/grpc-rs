#![feature(custom_inner_attributes)]
#![rustfmt::skip]
//! Synchronous, codec-parametric gRPC service definitions.
//!
//! `grpc_service` turns a trait into a handler trait, a client stub, a server
//! wrapper, and `MethodDescriptor` constants. `server_streaming`,
//! `client_streaming`, and `bidi_streaming` select cardinality. Streaming
//! payloads use `Vec` so the generated calls stay synchronous.

use proc_macro::TokenStream;

const OPT: &str = "::core::option::Option";
const COMP: &str = "::grpc_core::compression::CompressionAlgorithm";
const STATUS: &str = "::grpc_core::status::Status";

/// Builds synchronous client stubs, server handlers, and method descriptors.
///
/// `package = "demo.v1"` prefixes paths as `/demo.v1.Service/Method`. An empty
/// attribute uses `/Service/Method`.
#[proc_macro_attribute]
pub fn grpc_service(attr: TokenStream, item: TokenStream) -> TokenStream {
    let source = expand_source(&attr.to_string(), &item.to_string());
    source.parse().unwrap_or_else(|error| error_tokens(&format!("generated service failed to parse: {error}")))
}

fn expand_source(attr: &str, item: &str) -> String {
    match parse_package(attr).and_then(|package| parse_service(item).map(|service| emit(&package, &service))) {
        Ok(source) => source,
        Err(message) => compile_error_source(&message),
    }
}

fn compile_error_source(message: &str) -> String {
    let mut escaped = String::new();
    for ch in message.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' | '\r' => escaped.push(' '),
            _ => escaped.push(ch),
        }
    }
    format!("::core::compile_error!(\"{escaped}\");")
}

fn error_tokens(message: &str) -> TokenStream {
    compile_error_source(message).parse().unwrap_or_else(|_| TokenStream::new())
}

struct Service {
    vis: String,
    supers: String,
    name: String,
    methods: Vec<Method>,
}

struct Method {
    attrs: Vec<String>,
    name: String,
    request: String,
    response: String,
    cardinality: Cardinality,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cardinality {
    Unary,
    ServerStreaming,
    ClientStreaming,
    BidiStreaming,
}

impl Cardinality {
    fn named(name: &str) -> Option<Self> {
        match name {
            "unary" => Some(Self::Unary),
            "server_streaming" => Some(Self::ServerStreaming),
            "client_streaming" => Some(Self::ClientStreaming),
            "bidi_streaming" => Some(Self::BidiStreaming),
            _ => None,
        }
    }

    fn as_path(self) -> &'static str {
        match self {
            Self::Unary => "Unary",
            Self::ServerStreaming => "ServerStreaming",
            Self::ClientStreaming => "ClientStreaming",
            Self::BidiStreaming => "BidiStreaming",
        }
    }
}

#[derive(Clone)]
enum Tok {
    Ident(String),
    Sym(String),
    Str(String),
}

fn parse_package(attr: &str) -> Result<String, String> {
    let tokens = lex(attr)?;
    if tokens.is_empty() { return Ok(String::new()); }
    if !ident_is(&tokens, 0, "package") || !sym_is(&tokens, 1, "=") { return Err("expected `package = \"...\"`".to_string()); }
    let Some(package) = str_at(&tokens, 2) else { return Err("package must be a string literal".to_string()); };
    let mut end = 3;
    if sym_is(&tokens, end, ",") { end += 1; }
    if end != tokens.len() { return Err("unexpected tokens after package".to_string()); }
    if !valid_package(package) {
        return Err(format!("invalid package `{package}`"));
    }
    Ok(package.to_string())
}

fn parse_service(item: &str) -> Result<Service, String> {
    let tokens = lex(item)?;
    let mut index = skip_attributes(&tokens)?;
    let mut vis = String::new();
    if ident_is(&tokens, index, "pub") {
        index += 1;
        if sym_is(&tokens, index, "(") {
            let (next, inner) = take_group(&tokens, index, "(", ")")?;
            vis = format!("pub({}) ", render_tokens(inner));
            index = next;
        } else {
            vis = String::from("pub ");
        }
    }
    if ident_is(&tokens, index, "unsafe") { return Err("unsafe traits are not supported".to_string()); }
    if !ident_is(&tokens, index, "trait") { return Err("grpc_service expects a trait".to_string()); }
    index += 1;
    let Some(name) = ident_at(&tokens, index) else { return Err("trait is missing a name".to_string()); };
    let name = name.to_string();
    ensure_ident(&name, "trait")?;
    let body_at = tokens.iter().position(|token| sym(token, "{")).ok_or_else(|| "trait is missing a body".to_string())?;
    let head = &tokens[index + 1..body_at];
    if head.iter().any(|token| sym(token, "<") || ident_token(token) == Some("where")) { return Err("generic service traits are not supported".to_string()); }
    let supers = render_tokens(head);
    let (_, body) = take_group(&tokens, body_at, "{", "}")?;
    let mut methods = Vec::new();
    for method in split_methods(body)? {
        methods.push(parse_method(&method)?);
    }
    let service = Service { vis, supers, name, methods };
    reject_collisions(&service)?;
    Ok(service)
}

fn skip_attributes(tokens: &[Tok]) -> Result<usize, String> {
    let mut index = 0;
    while sym_is(tokens, index, "#") {
        let (next, _) = take_group(tokens, index + 1, "[", "]")?;
        index = next;
    }
    Ok(index)
}

fn split_methods(tokens: &[Tok]) -> Result<Vec<Vec<Tok>>, String> {
    let mut methods = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0i32;
    for token in tokens {
        if matches!(token, Tok::Sym(sym) if sym == "(" || sym == "[" || sym == "{") { depth += 1; }
        if matches!(token, Tok::Sym(sym) if sym == ")" || sym == "]" || sym == "}") { depth -= 1; }
        if depth == 0 && sym(token, ";") {
            if current.is_empty() { return Err("empty method declaration".to_string()); }
            methods.push(std::mem::take(&mut current));
        } else {
            current.push(token.clone());
        }
    }
    if current.is_empty() { Ok(methods) } else { Err("method declaration must end with ';'".to_string()) }
}

fn parse_method(tokens: &[Tok]) -> Result<Method, String> {
    let mut index = 0;
    let mut attrs = Vec::new();
    let mut flags = Vec::new();
    while sym_is(tokens, index, "#") {
        let (next, inner) = take_group(tokens, index + 1, "[", "]")?;
        if let Some(found) = bare_cardinality(inner) {
            flags.push(found);
        } else {
            attrs.push(format!("#[{}]", render_tokens(inner)));
        }
        index = next;
    }
    let cardinality = resolve_cardinality(&flags)?;
    if ident_is(tokens, index, "\x61sync") { return Err("synchronous services cannot declare suspended methods".to_string()); }
    if !ident_is(tokens, index, "fn") { return Err("expected a method".to_string()); }
    index += 1;
    let Some(name) = ident_at(tokens, index) else { return Err("method is missing a name".to_string()); };
    let name = name.to_string();
    ensure_ident(&name, "method")?;
    index += 1;
    if sym_is(tokens, index, "<") { return Err("generic methods are not supported".to_string()); }
    if !sym_is(tokens, index, "(") {
        return Err(format!("method {name} is missing request parentheses"));
    }
    let (next, args) = take_group(tokens, index, "(", ")")?;
    let request = argument_type(args)?;
    index = next;
    let arrow = tokens[index..].iter().position(|token| sym(token, "->")).ok_or_else(|| format!("method {name} is missing ->"))?;
    let response_tokens = &tokens[index + arrow + 1..];
    if response_tokens.iter().any(|token| ident_token(token) == Some("where")) {
        return Err(format!("method {name} cannot declare a where clause"));
    }
    let response = render_type(response_tokens)?;
    Ok(Method { attrs, name, request, response, cardinality })
}

fn bare_cardinality(tokens: &[Tok]) -> Option<Cardinality> {
    let mut last = None;
    let mut want_ident = true;
    for token in tokens {
        if want_ident {
            last = Some(ident_token(token)?);
            want_ident = false;
        } else if sym(token, "::") {
            want_ident = true;
        } else {
            return None;
        }
    }
    if want_ident { None } else { last.and_then(Cardinality::named) }
}

fn resolve_cardinality(flags: &[Cardinality]) -> Result<Cardinality, String> {
    let mut client = false;
    let mut server = false;
    let mut bidi = false;
    for flag in flags {
        match flag {
            Cardinality::Unary => {}
            Cardinality::ClientStreaming => client = true,
            Cardinality::ServerStreaming => server = true,
            Cardinality::BidiStreaming => bidi = true,
        }
    }
    if bidi && (client || server) { return Err("conflicting RPC cardinality attributes".to_string()); }
    Ok(match (client, server, bidi) {
        (_, _, true) | (true, true, _) => Cardinality::BidiStreaming,
        (true, false, _) => Cardinality::ClientStreaming,
        (false, true, _) => Cardinality::ServerStreaming,
        _ => Cardinality::Unary,
    })
}

fn reject_collisions(service: &Service) -> Result<(), String> {
    let mut consts = Vec::new();
    let mut rpcs = Vec::new();
    for method in &service.methods {
        if is_reserved(&method.name) {
            return Err(format!("method `{}` collides with generated client methods", method.name));
        }
        let constant = const_name(&service.name, &method.name);
        let rpc = rpc_ident(&method.name);
        let const_taken = consts.iter().any(|seen| seen == &constant);
        let rpc_taken = rpcs.iter().any(|seen| seen == &rpc);
        if const_taken || rpc_taken {
            return Err(format!("method `{}` duplicates a method descriptor", method.name));
        }
        consts.push(constant);
        rpcs.push(rpc);
    }
    Ok(())
}

fn is_reserved(name: &str) -> bool {
    matches!(cooked(name), "new" | "with_codec" | "with_compression" | "codec" | "compression" | "transport" | "handler")
}

fn argument_type(tokens: &[Tok]) -> Result<String, String> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0i32;
    for token in tokens {
        if sym(token, "<") || sym(token, "(") || sym(token, "[") { depth += 1; }
        if sym(token, ">") || sym(token, ")") || sym(token, "]") { depth -= 1; }
        if depth == 0 && sym(token, ",") {
            if !current.is_empty() { parts.push(std::mem::take(&mut current)); }
            continue;
        }
        current.push(token.clone());
    }
    if !current.is_empty() { parts.push(current); }
    let mut messages = Vec::new();
    for part in &parts {
        if is_receiver(part) { continue; }
        messages.push(arg_ty(part)?);
    }
    match &messages[..] {
        [ty] => Ok(ty.clone()),
        _ => Err("method accepts one request type".to_string()),
    }
}

fn is_receiver(tokens: &[Tok]) -> bool {
    match tokens.first() {
        Some(Tok::Ident(name)) if name == "self" => true,
        Some(Tok::Ident(name)) if name == "mut" => matches!(tokens.get(1), Some(Tok::Ident(next)) if next == "self"),
        Some(Tok::Sym(amp)) if amp == "&" => {
            matches!(tokens.get(1), Some(Tok::Ident(name)) if name == "self")
                || matches!((tokens.get(1), tokens.get(2)), (Some(Tok::Ident(mode)), Some(Tok::Ident(name))) if mode == "mut" && name == "self")
        }
        _ => false,
    }
}

fn arg_ty(tokens: &[Tok]) -> Result<String, String> {
    let mut depth = 0i32;
    for (index, token) in tokens.iter().enumerate() {
        if sym(token, "<") || sym(token, "(") || sym(token, "[") { depth += 1; }
        if sym(token, ">") || sym(token, ")") || sym(token, "]") { depth -= 1; }
        if depth == 0 && sym(token, ":") { return render_type(&tokens[index + 1..]); }
    }
    render_type(tokens)
}

fn render_type(tokens: &[Tok]) -> Result<String, String> {
    if tokens.is_empty() { Err("missing message type".to_string()) } else { Ok(render_tokens(tokens)) }
}

fn push_escaped(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
}

fn render_tokens(tokens: &[Tok]) -> String {
    let mut out = String::new();
    let mut word = false;
    for token in tokens {
        match token {
            Tok::Ident(text) => {
                if word { out.push(' '); }
                out.push_str(text);
                word = true;
            }
            Tok::Str(text) => {
                if word { out.push(' '); }
                out.push('"');
                push_escaped(&mut out, text);
                out.push('"');
                word = true;
            }
            Tok::Sym(sym) => {
                out.push_str(sym);
                word = false;
            }
        }
    }
    out
}

fn lex(source: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = source.chars().collect();
    let mut index = 0;
    let mut tokens = Vec::new();
    while index < chars.len() {
        let ch = chars[index];
        if ch.is_whitespace() { index += 1; continue; }
        if ch == '/' && chars.get(index + 1) == Some(&'/') {
            while index < chars.len() && chars[index] != '\n' { index += 1; }
            continue;
        }
        if ch == '/' && chars.get(index + 1) == Some(&'*') {
            index += 2;
            while index + 1 < chars.len() && !(chars[index] == '*' && chars[index + 1] == '/') { index += 1; }
            if index + 1 >= chars.len() { return Err("unterminated block comment".to_string()); }
            index += 2;
            continue;
        }
        if let Some((quote_at, raw, hashes)) = string_prefix(&chars, index) {
            let (next, text) = if raw { read_raw(&chars, quote_at + 1, hashes)? } else { read_string(&chars, quote_at)? };
            tokens.push(Tok::Str(text));
            index = next;
            continue;
        }
        if ch == '"' {
            let (next, text) = read_string(&chars, index)?;
            tokens.push(Tok::Str(text));
            index = next;
            continue;
        }
        if ch == 'r' && chars.get(index + 1) == Some(&'#') && chars.get(index + 2).is_some_and(|next| *next == '_' || next.is_ascii_alphabetic()) {
            index += 2;
            let start = index;
            while index < chars.len() && (chars[index].is_ascii_alphanumeric() || chars[index] == '_') { index += 1; }
            let mut name = String::from("r#");
            name.extend(chars[start..index].iter());
            tokens.push(Tok::Ident(name));
            continue;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = index;
            index += 1;
            while index < chars.len() && (chars[index].is_ascii_alphanumeric() || chars[index] == '_') { index += 1; }
            tokens.push(Tok::Ident(chars[start..index].iter().collect()));
            continue;
        }
        if ch == ':' && chars.get(index + 1) == Some(&':') { tokens.push(Tok::Sym("::".to_string())); index += 2; continue; }
        if ch == '-' && chars.get(index + 1) == Some(&'>') { tokens.push(Tok::Sym("->".to_string())); index += 2; continue; }
        tokens.push(Tok::Sym(ch.to_string()));
        index += 1;
    }
    Ok(tokens)
}

fn string_prefix(chars: &[char], index: usize) -> Option<(usize, bool, usize)> {
    let bytes = chars.get(index..)?;
    let mut cursor = 0;
    if matches!(bytes.first(), Some('b' | 'c')) { cursor = 1; }
    let mut raw = false;
    if bytes.get(cursor) == Some(&'r') { raw = true; cursor += 1; }
    let mut hashes = 0usize;
    if raw {
        while bytes.get(cursor) == Some(&'#') { hashes += 1; cursor += 1; }
    }
    if cursor == 0 || bytes.get(cursor) != Some(&'"') { None } else { Some((index + cursor, raw, hashes)) }
}

fn read_raw(chars: &[char], mut index: usize, hashes: usize) -> Result<(usize, String), String> {
    let mut text = String::new();
    while index < chars.len() {
        if chars[index] == '"' && (0..hashes).all(|offset| chars.get(index + 1 + offset) == Some(&'#')) { return Ok((index + 1 + hashes, text)); }
        text.push(chars[index]);
        index += 1;
    }
    Err("unterminated string".to_string())
}

fn read_string(chars: &[char], start: usize) -> Result<(usize, String), String> {
    let mut index = start + 1;
    let mut text = String::new();
    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            let (decoded, next) = decode_escape(chars, index)?;
            text.push(decoded);
            index = next;
            continue;
        }
        if ch == '"' { return Ok((index + 1, text)); }
        text.push(ch);
        index += 1;
    }
    Err("unterminated string".to_string())
}

fn decode_escape(chars: &[char], index: usize) -> Result<(char, usize), String> {
    let Some(kind) = chars.get(index + 1).copied() else { return Err("unterminated string".to_string()); };
    let simple = match kind { 'n' => Some('\n'), 'r' => Some('\r'), 't' => Some('\t'), '\\' => Some('\\'), '\'' => Some('\''), '"' => Some('"'), '0' => Some('\0'), _ => None };
    if let Some(ch) = simple { return Ok((ch, index + 2)); }
    if kind == 'x' {
        let hex = (0..2).map(|offset| chars.get(index + 2 + offset).copied()).collect::<Option<String>>().filter(|hex| hex.chars().all(|ch| ch.is_ascii_hexdigit())).ok_or_else(|| "bad hex escape".to_string())?;
        return Ok((char::from(u8::from_str_radix(&hex, 16).map_err(|_| "bad hex escape".to_string())?), index + 4));
    }
    if kind == 'u' && chars.get(index + 2) == Some(&'{') {
        let end = chars[index + 3..].iter().position(|ch| *ch == '}').ok_or_else(|| "bad unicode escape".to_string())?;
        let hex: String = chars[index + 3..index + 3 + end].iter().collect();
        if (1..=6).contains(&hex.len())
            && let Ok(code) = u32::from_str_radix(&hex, 16)
            && let Some(ch) = char::from_u32(code)
        {
            return Ok((ch, index + 4 + end));
        }
    }
    Err("unknown string escape".to_string())
}

fn take_group<'a>(tokens: &'a [Tok], start: usize, open: &str, close: &str) -> Result<(usize, &'a [Tok]), String> {
    if !sym_is(tokens, start, open) { return Err(format!("expected `{open}`")); }
    let mut depth = 0i32;
    for (offset, token) in tokens[start..].iter().enumerate() {
        if sym(token, open) { depth += 1; }
        if sym(token, close) {
            depth -= 1;
            if depth == 0 { return Ok((start + offset + 1, &tokens[start + 1..start + offset])); }
        }
    }
    Err(format!("unbalanced `{open}`"))
}

fn ident_is(tokens: &[Tok], index: usize, expected: &str) -> bool { ident_at(tokens, index) == Some(expected) }
fn ident_at(tokens: &[Tok], index: usize) -> Option<&str> { tokens.get(index).and_then(ident_token) }
fn ident_token(token: &Tok) -> Option<&str> { match token { Tok::Ident(text) => Some(text.as_str()), _ => None } }
fn sym_is(tokens: &[Tok], index: usize, expected: &str) -> bool { tokens.get(index).is_some_and(|token| sym(token, expected)) }
fn sym(token: &Tok, expected: &str) -> bool { matches!(token, Tok::Sym(text) if text == expected) }
fn str_at(tokens: &[Tok], index: usize) -> Option<&str> { match tokens.get(index) { Some(Tok::Str(text)) => Some(text.as_str()), _ => None } }

fn emit(package: &str, service: &Service) -> String {
    let mut out = String::new();
    emit_support(&mut out, service); emit_constants(&mut out, package, service); emit_handler(&mut out, service);
    emit_channel(&mut out, service); emit_client(&mut out, service); emit_server(&mut out, package, service);
    out
}

fn emit_support(out: &mut String, service: &Service) {
    let module = support_module(service);
    put(out, &format!("{}mod {module} {{", visibility(service)));
    put(out, "    #[derive(Clone, Copy, Debug, Default)]");
    put(out, "    pub struct DefaultCodec;");
    put(out, "    pub trait ServiceHandler {");
    put(out, "        fn service_name(&self) -> &'static str;");
    let descriptors = "fn descriptors(&self) -> &'static [::grpc_core::MethodDescriptor];";
    put(out, &format!("        {descriptors}"));
    put(out, "    }");
    put(out, "}");
}

fn emit_constants(out: &mut String, package: &str, service: &Service) {
    let vis = visibility(service);
    for method in &service.methods {
        for attr in &method.attrs {
            put(out, attr);
        }
        let constant = const_name(&service.name, &method.name);
        let path = method_path(package, &service.name, &method.name);
        let card = method.cardinality.as_path();
        put(out, &format!("{vis}const {constant}: ::grpc_core::MethodDescriptor ="));
        put(out, "    ::grpc_core::MethodDescriptor::new(");
        put(out, &format!("        \"{path}\","));
        put(out, &format!("        ::grpc_core::RpcCardinality::{card},"));
        put(out, "    );");
    }
}

fn emit_handler(out: &mut String, service: &Service) {
    put(out, &format!("{}trait {}{} {{", visibility(service), service.name, service.supers));
    for method in &service.methods {
        emit_signature(out, method, false, false);
    }
    put(out, "}");
}

fn emit_channel(out: &mut String, service: &Service) {
    put(out, &format!("{}trait {}Channel<C> {{", visibility(service), service.name));
    for method in &service.methods {
        for attr in &method.attrs {
            put(out, &format!("    {attr}"));
        }
        let request = request_ty(method);
        let response = response_ty(method);
        put(out, &format!("    fn {}(", method.name));
        put(out, "        &mut self,");
        put(out, "        codec: &mut C,");
        put(out, "        method: &'static ::grpc_core::MethodDescriptor,");
        put(out, &format!("        compression: &{OPT}<{COMP}>,"));
        put(out, &format!("        request: {request},"));
        put(out, &format!("    ) -> ::core::result::Result<{response}, {STATUS}>;"));
    }
    put(out, "}");
}

fn emit_client(out: &mut String, service: &Service) {
    emit_shell(out, service, false);
    if service.methods.is_empty() { return; }
    let client = format!("{}Client", service.name);
    let channel = format!("{}Channel", service.name);
    put(out, &format!("impl<T, C> {client}<T, C> where T: {channel}<C> {{"));
    for method in &service.methods {
        emit_signature(out, method, true, true);
        let constant = const_name(&service.name, &method.name);
        put(out, &format!("        {channel}::{}(\n            &mut self.transport,\n            &mut self.codec,\n            &{constant},\n            &self.compression,\n            request,\n        )", method.name));
        put(out, "    }");
    }
    put(out, "}");
}

fn emit_server(out: &mut String, package: &str, service: &Service) {
    emit_shell(out, service, true);
    let name = &service.name;
    let server = format!("{name}Server");
    let module = support_module(service);
    let qualified = qualified_service(package, name);
    put(out, &format!("impl<S, C> {module}::ServiceHandler for {server}<S, C>\nwhere\n    S: {name},\n{{\n    fn service_name(&self) -> &'static str {{\n        \"{qualified}\"\n    }}\n    fn descriptors(&self) -> &'static [::grpc_core::MethodDescriptor] {{\n        &["));
    for method in &service.methods {
        for attr in &method.attrs { put(out, &format!("            {attr}")); }
        put(out, &format!("            {},", const_name(name, &method.name)));
    }
    put(out, "        ]\n    }\n}");
    if service.methods.is_empty() { return; }
    put(out, &format!("impl<S, C> {server}<S, C> where S: {name} {{"));
    for method in &service.methods {
        emit_signature(out, method, true, true);
        put(out, &format!("        self.handler.{}(request)\n    }}", method.name));
    }
    put(out, "}");
}

fn emit_shell(out: &mut String, service: &Service, server: bool) {
    let vis = visibility(service);
    let module = support_module(service);
    let (ty, param, field) = if server { (format!("{}Server", service.name), "S", "handler") } else { (format!("{}Client", service.name), "T", "transport") };
    out.push_str(&format!("{vis}struct {ty}<{param}, C = {module}::DefaultCodec> {{\n    {field}: {param},\n    codec: C,\n    compression: {OPT}<{COMP}>,\n}}\nimpl<{param}, C> {ty}<{param}, C> {{\n    pub fn new({field}: {param}, codec: C) -> Self {{\n        Self {{ {field}, codec, compression: {OPT}::None }}\n    }}\n    pub fn with_codec<NewCodec>(self) -> {ty}<{param}, NewCodec>\n    where\n        NewCodec: ::core::default::Default,\n    {{\n        {ty} {{\n            {field}: self.{field},\n            codec: ::core::default::Default::default(),\n            compression: self.compression,\n        }}\n    }}\n    pub fn with_compression(mut self, algorithm: {COMP}) -> Self {{\n        self.compression = {OPT}::Some(algorithm);\n        self\n    }}\n    pub fn codec(&self) -> &C {{ &self.codec }}\n    pub fn {field}(&self) -> &{param} {{ &self.{field} }}\n    pub fn compression(&self) -> {OPT}<&{COMP}> {{ self.compression.as_ref() }}\n}}\n"));
}

fn emit_signature(out: &mut String, method: &Method, public: bool, body: bool) {
    let prefix = if public { "pub " } else { "" };
    let tail = if body { "{" } else { ";" };
    let request = request_ty(method);
    let response = response_ty(method);
    for attr in &method.attrs {
        put(out, &format!("    {attr}"));
    }
    put(out, &format!("    {prefix}fn {}(&mut self, request: {request})", method.name));
    put(out, &format!("    -> ::core::result::Result<{response}, {STATUS}> {tail}"));
}

fn request_ty(method: &Method) -> String {
    envelope(
        &method.request,
        matches!(method.cardinality, Cardinality::ClientStreaming | Cardinality::BidiStreaming),
        false,
    )
}

fn response_ty(method: &Method) -> String {
    envelope(
        &method.response,
        matches!(method.cardinality, Cardinality::ServerStreaming | Cardinality::BidiStreaming),
        true,
    )
}

fn envelope(inner: &str, stream: bool, response: bool) -> String {
    let wrapper = if response { "Response" } else { "Request" };
    if stream { format!("::grpc_core::{wrapper}<::std::vec::Vec<{inner}>>") } else { format!("::grpc_core::{wrapper}<{inner}>") }
}
fn put(out: &mut String, line: &str) { out.push_str(line); out.push('\n'); }
fn visibility(service: &Service) -> &str { &service.vis }
fn support_module(service: &Service) -> String { format!("{}_grpc_support", reshape(cooked(&service.name), false)) }
fn const_name(service: &str, method: &str) -> String { format!("{}_{}", reshape(cooked(service), true), reshape(cooked(method), true)) }
fn cooked(name: &str) -> &str { name.strip_prefix("r#").unwrap_or(name) }
fn qualified_service(package: &str, service: &str) -> String { if package.is_empty() { service.to_string() } else { format!("{package}.{service}") } }
fn method_path(package: &str, service: &str, method: &str) -> String { format!("/{}/{}", qualified_service(package, service), rpc_ident(method)) }

fn rpc_ident(name: &str) -> String {
    let name = cooked(name);
    let head = name.chars().next().is_some_and(|ch| ch.is_ascii_uppercase());
    if name.contains('_') || !head {
        let mut out = String::new();
        for part in name.split('_').filter(|part| !part.is_empty()) {
            out.push_str(&capitalize(part));
        }
        if out.is_empty() { name.to_string() } else { out }
    } else {
        name.to_string()
    }
}

fn capitalize(part: &str) -> String {
    let mut chars = part.chars();
    let Some(first) = chars.next() else { return String::new(); };
    let mut out = String::new();
    out.extend(first.to_uppercase());
    out.push_str(chars.as_str());
    out
}

fn reshape(name: &str, ascii_upper: bool) -> String {
    let mut out = String::new();
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() && index > 0 { out.push('_'); }
        out.push(if ascii_upper { ch.to_ascii_uppercase() } else { ch.to_ascii_lowercase() });
    }
    out
}
fn ensure_ident(name: &str, role: &str) -> Result<(), String> {
    let bare = cooked(name);
    let raw = name.starts_with("r#");
    if (name == bare || raw) && is_rust_ident(bare) && (raw || !is_keyword(bare)) { Ok(()) } else { Err(format!("{role} `{name}` is not a usable identifier")) }
}
fn valid_package(package: &str) -> bool { !package.is_empty() && package.split('.').all(|part| !part.is_empty() && is_rust_ident(part)) }
fn is_rust_ident(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(ch) if ch == '_' || ch.is_ascii_alphabetic()) && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
fn is_keyword(name: &str) -> bool {
    " as \x61sync \x61wait break const continue crate dyn else enum extern false fn for if impl in let loop match mod move mut pub ref return self Self static struct super trait true type unsafe use where while gen union abstract become box do final macro override priv typeof unsized virtual yield try ".contains(&format!(" {name} "))
}

#[cfg(test)]
mod tests {
    use super::expand_source;

    fn assert_has(output: &str, needle: &str) {
        assert!(output.contains(needle), "missing {needle} in {output}");
    }

    #[test]
    fn expands_all_cardinalities() {
        let output = expand_source(
            r#"package = "demo.v1""#,
            r#"
            pub trait Echo {
                fn unary(Ping) -> Pong;
                #[server_streaming]
                fn server(Ping) -> Pong;
                #[client_streaming]
                fn client(Ping) -> Pong;
                #[bidi_streaming]
                fn bidi(Ping) -> Pong;
            }
            "#,
        );
        for needle in [
            "struct EchoClient", "struct EchoServer", "trait Echo", "trait ServiceHandler", "trait EchoChannel",
            "fn with_codec", "fn with_compression", "DefaultCodec", "MethodDescriptor", "CompressionAlgorithm",
            "Unary", "ServerStreaming", "ClientStreaming", "BidiStreaming", "\"/demo.v1.Echo/Unary\"",
            "\"/demo.v1.Echo/Server\"", "\"/demo.v1.Echo/Client\"", "\"/demo.v1.Echo/Bidi\"", "Vec",
        ] {
            assert_has(&output, needle);
        }
        assert!(!output.contains("compile_error"), "{output}");
    }

    #[test]
    fn merges_stream_directions_and_reports_parse_errors() {
        let output = expand_source("", "trait Echo { #[client_streaming] #[server_streaming] fn chat(Ping) -> Pong; }");
        assert_has(&output, "BidiStreaming");
        assert_has(&output, "\"/Echo/Chat\"");
        let conflict = expand_source("", "trait Echo { #[bidi_streaming] #[server_streaming] fn chat(Ping) -> Pong; }");
        assert_has(&conflict, "compile_error");
        let reversed = expand_source("", "trait Echo { #[client_streaming] #[server_streaming] #[bidi_streaming] fn chat(Ping) -> Pong; }");
        assert_has(&reversed, "compile_error");
        assert_has(&expand_source("", "fn not_a_trait() {}"), "compile_error");
    }

    #[test]
    fn rejects_bad_shapes_and_compiles_returns() {
        let cfg = expand_source("", "trait Echo { #[cfg(not(server_streaming))] fn unary(Ping) -> Pong; }");
        assert!(cfg.contains("#[cfg(not(server_streaming))]"), "{cfg}");
        assert!(!cfg.contains("ServerStreaming"), "{cfg}");
        assert_has(&expand_source("", "pub(crate) trait Echo { fn unary(Ping) -> Pong; }"), "pub(crate) struct EchoClient");
        assert_has(&expand_source(r#"package = r"demo.v1""#, "trait Echo { fn unary(Ping) -> Pong; }"), "\"/demo.v1.Echo/Unary\"");
        assert_has(&expand_source(r#"package = "demo\u{2e}v1""#, "trait Echo { fn unary(Ping) -> Pong; }"), "\"/demo.v1.Echo/Unary\"");
        assert_has(&expand_source("", "trait Echo { fn getHTTP(Ping) -> Pong; }"), "\"/Echo/GetHTTP\"");
        assert_has(&expand_source("", "trait Echo: Send { fn unary(Ping) -> Pong; }"), "trait Echo:Send {");
        assert_has(&expand_source("", "trait Echo { fn r#type(Ping) -> Pong; }"), "fn r#type(");
        assert_has(&expand_source(r#"package = "demo.v1""#, "pub trait Echo { fn say_hello(request: demo::Ping) -> demo::Pong; }"), "\"/demo.v1.Echo/SayHello\"");
        let unary = expand_source("", "trait Echo { fn unary(Ping) -> Pong; }");
        assert_has(&unary, "\"/Echo/Unary\"");
        assert!(!unary.contains("Vec") && !unary.contains("pub struct EchoClient"), "{unary}");
        for item in ["fn unary(request: Ping, ctx: u64) -> Pong;", "fn unary(Ping) -> Pong where Pong: Send;", "fn with_codec(Ping) -> Pong;", "fn foo2(Ping) -> Pong; fn foo_2(Ping) -> Pong;", "fn unary<T>(T) -> T;", "fn only(&mut self) -> Pong;"] {
            assert_has(&expand_source("", &format!("trait Echo {{ {item} }}")), "compile_error");
        }
        let output = expand_source(r#"package = "demo.v1""#, "pub trait Echo { fn unary(Ping) -> Pong; #[server_streaming] fn server(Ping) -> Pong; }");
        assert!(output.contains("self.handler.unary(request)\n"), "{output}");
        assert!(output.contains("request,\n        )\n"), "{output}");
        assert!(!output.contains("compile_error"), "{output}");
        compile_generated(&output);
    }

    fn compile_generated(generated: &str) {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_nanos()).unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("grpc-macros-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("dir");
        let core = dir.join("core.rs");
        let probe = dir.join("probe.rs");
        std::fs::write(&core, "#[derive(Clone, Copy)] pub struct MethodDescriptor; impl MethodDescriptor { pub const fn new(_: &'static str, _: RpcCardinality) -> Self { Self } } #[derive(Clone, Copy)] pub enum RpcCardinality { Unary, ServerStreaming, ClientStreaming, BidiStreaming } pub struct Request<T> { _m: ::core::marker::PhantomData<T> } pub struct Response<T> { _m: ::core::marker::PhantomData<T> } pub mod compression { #[derive(Clone, Copy)] pub struct CompressionAlgorithm; } pub mod status { pub struct Status; }").expect("core");
        std::fs::write(&probe, format!("pub struct Ping; pub struct Pong;\n{generated}")).expect("probe");
        let rlib = dir.join("libgrpc_core.rlib");
        let built = std::process::Command::new("rustc").args(["--edition=2024", "--crate-type=lib", "--crate-name=grpc_core", "-Dwarnings"]).arg(&core).arg("-o").arg(&rlib).output().expect("rustc");
        assert!(built.status.success(), "{}", String::from_utf8_lossy(&built.stderr));
        let checked = std::process::Command::new("rustc").args(["--edition=2024", "--crate-type=lib", "--crate-name=probe", "-Dwarnings", "--extern"]).arg(format!("grpc_core={}", rlib.display())).arg(&probe).arg("--out-dir").arg(&dir).output().expect("rustc");
        assert!(checked.status.success(), "{}", String::from_utf8_lossy(&checked.stderr));
        std::fs::remove_dir_all(dir).expect("cleanup");
    }
}
