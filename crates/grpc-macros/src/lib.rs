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
    is_pub: bool,
    name: String,
    methods: Vec<Method>,
}

struct Method {
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

    fn merge(self, other: Self) -> Result<Self, String> {
        match (self, other) {
            (left, right) if left == right => Ok(left),
            (Self::Unary, value) | (value, Self::Unary) => Ok(value),
            (Self::ClientStreaming, Self::ServerStreaming) | (Self::ServerStreaming, Self::ClientStreaming) => Ok(Self::BidiStreaming),
            _ => Err("conflicting RPC cardinality attributes".to_string()),
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
    if tokens.is_empty() {
        return Ok(String::new());
    }
    if !ident_is(&tokens, 0, "package") || !sym_is(&tokens, 1, "=") {
        return Err("expected `package = \"...\"`".to_string());
    }
    let Some(package) = str_at(&tokens, 2) else {
        return Err("package must be a string literal".to_string());
    };
    let mut end = 3;
    if sym_is(&tokens, end, ",") {
        end += 1;
    }
    if end != tokens.len() {
        return Err("unexpected tokens after package".to_string());
    }
    if !valid_package(package) {
        return Err(format!("invalid package `{package}`"));
    }
    Ok(package.to_string())
}

fn parse_service(item: &str) -> Result<Service, String> {
    let tokens = lex(item)?;
    let mut index = skip_attributes(&tokens)?;
    let mut is_pub = false;
    if ident_is(&tokens, index, "pub") {
        is_pub = true;
        index += 1;
        if sym_is(&tokens, index, "(") {
            let (next, _) = take_group(&tokens, index, "(", ")")?;
            index = next;
        }
    }
    if ident_is(&tokens, index, "unsafe") {
        return Err("unsafe traits are not supported".to_string());
    }
    if !ident_is(&tokens, index, "trait") {
        return Err("grpc_service expects a trait".to_string());
    }
    index += 1;
    let Some(name) = ident_at(&tokens, index) else {
        return Err("trait is missing a name".to_string());
    };
    let name = name.to_string();
    ensure_ident(&name, "trait")?;
    if has_generics(&tokens, index) {
        return Err("generic service traits are not supported".to_string());
    }
    let body_at = tokens.iter().position(|token| sym(token, "{")).ok_or_else(|| "trait is missing a body".to_string())?;
    let (_, body) = take_group(&tokens, body_at, "{", "}")?;
    let mut methods = Vec::new();
    for method in split_methods(body)? {
        methods.push(parse_method(&method)?);
    }
    Ok(Service { is_pub, name, methods })
}

fn skip_attributes(tokens: &[Tok]) -> Result<usize, String> {
    let mut index = 0;
    while sym_is(tokens, index, "#") {
        let (next, _) = take_group(tokens, index + 1, "[", "]")?;
        index = next;
    }
    Ok(index)
}

fn has_generics(tokens: &[Tok], name_index: usize) -> bool {
    for token in &tokens[name_index + 1..] {
        if sym(token, "{") {
            return false;
        }
        if sym(token, "<") || ident_token(token) == Some("where") {
            return true;
        }
    }
    false
}

fn split_methods(tokens: &[Tok]) -> Result<Vec<Vec<Tok>>, String> {
    let mut methods = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0i32;
    for token in tokens {
        if matches!(token, Tok::Sym(sym) if sym == "(" || sym == "[" || sym == "{") {
            depth += 1;
        }
        if matches!(token, Tok::Sym(sym) if sym == ")" || sym == "]" || sym == "}") {
            depth -= 1;
        }
        if depth == 0 && sym(token, ";") {
            if current.is_empty() {
                return Err("empty method declaration".to_string());
            }
            methods.push(std::mem::take(&mut current));
        } else {
            current.push(token.clone());
        }
    }
    if current.is_empty() { Ok(methods) } else { Err("method declaration must end with ';'".to_string()) }
}

fn parse_method(tokens: &[Tok]) -> Result<Method, String> {
    let mut index = 0;
    let mut cardinality = Cardinality::Unary;
    while sym_is(tokens, index, "#") {
        let (next, inner) = take_group(tokens, index + 1, "[", "]")?;
        if let Some(found) = cardinality_in(inner)? {
            cardinality = cardinality.merge(found)?;
        }
        index = next;
    }
    if ident_is(tokens, index, "async") {
        return Err("synchronous services cannot declare async methods".to_string());
    }
    if !ident_is(tokens, index, "fn") {
        return Err("expected a method".to_string());
    }
    index += 1;
    let Some(name) = ident_at(tokens, index) else {
        return Err("method is missing a name".to_string());
    };
    let name = name.to_string();
    ensure_ident(&name, "method")?;
    index += 1;
    if !sym_is(tokens, index, "(") {
        return Err(format!("method {name} is missing request parentheses"));
    }
    let (next, args) = take_group(tokens, index, "(", ")")?;
    let request = argument_type(args)?;
    index = next;
    let arrow = tokens[index..].iter().position(|token| sym(token, "->")).ok_or_else(|| format!("method {name} is missing ->"))?;
    let response = render_type(&tokens[index + arrow + 1..])?;
    Ok(Method { name, request, response, cardinality })
}

fn cardinality_in(tokens: &[Tok]) -> Result<Option<Cardinality>, String> {
    let mut found: Option<Cardinality> = None;
    for token in tokens {
        let Some(next) = ident_token(token).and_then(Cardinality::named) else { continue };
        found = Some(match found {
            None => next,
            Some(current) => current.merge(next)?,
        });
    }
    Ok(found)
}

fn argument_type(tokens: &[Tok]) -> Result<String, String> {
    if tokens.is_empty() {
        return Err("method requires a request type".to_string());
    }
    let mut depth = 0i32;
    let mut colon = None;
    for (index, token) in tokens.iter().enumerate() {
        if sym(token, "<") || sym(token, "(") || sym(token, "[") { depth += 1; }
        if sym(token, ">") || sym(token, ")") || sym(token, "]") { depth -= 1; }
        if depth == 0 && sym(token, ":") { colon = Some(index); }
    }
    let start = colon.map_or(0, |index| index + 1);
    render_type(&tokens[start..])
}

fn render_type(tokens: &[Tok]) -> Result<String, String> {
    if tokens.is_empty() { Err("missing message type".to_string()) } else { Ok(render_tokens(tokens)) }
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
                out.push_str(text);
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
        if ch == '"' {
            let (next, text) = read_string(&chars, index)?;
            tokens.push(Tok::Str(text));
            index = next;
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

fn read_string(chars: &[char], start: usize) -> Result<(usize, String), String> {
    let mut index = start + 1;
    let mut text = String::new();
    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' {
            let Some(escaped) = chars.get(index + 1).copied() else { break };
            text.push(match escaped { 'n' => '\n', 't' => '\t', 'r' => '\r', other => other });
            index += 2;
            continue;
        }
        if ch == '"' { return Ok((index + 1, text)); }
        text.push(ch);
        index += 1;
    }
    Err("unterminated string".to_string())
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

fn ident_is(tokens: &[Tok], index: usize, expected: &str) -> bool {
    ident_at(tokens, index) == Some(expected)
}

fn ident_at(tokens: &[Tok], index: usize) -> Option<&str> {
    tokens.get(index).and_then(ident_token)
}

fn ident_token(token: &Tok) -> Option<&str> {
    match token { Tok::Ident(text) => Some(text.as_str()), _ => None }
}

fn sym_is(tokens: &[Tok], index: usize, expected: &str) -> bool {
    tokens.get(index).is_some_and(|token| sym(token, expected))
}

fn sym(token: &Tok, expected: &str) -> bool {
    matches!(token, Tok::Sym(text) if text == expected)
}

fn str_at(tokens: &[Tok], index: usize) -> Option<&str> {
    match tokens.get(index) { Some(Tok::Str(text)) => Some(text.as_str()), _ => None }
}

fn emit(package: &str, service: &Service) -> String {
    let mut out = String::new();
    emit_support(&mut out, service);
    emit_constants(&mut out, package, service);
    emit_handler(&mut out, service);
    emit_channel(&mut out, service);
    emit_client(&mut out, service);
    emit_server(&mut out, package, service);
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
    let mut names = Vec::new();
    for method in &service.methods {
        let constant = const_name(&service.name, &method.name);
        let path = method_path(package, &service.name, &method.name);
        let card = method.cardinality.as_path();
        put(out, &format!("{vis}const {constant}: ::grpc_core::MethodDescriptor ="));
        put(out, "    ::grpc_core::MethodDescriptor::new(");
        put(out, &format!("        \"{path}\","));
        put(out, &format!("        ::grpc_core::RpcCardinality::{card},"));
        put(out, "    );");
        names.push(constant);
    }
    let table = descriptor_table(&service.name);
    let joined = names.join(", ");
    put(out, &format!("{vis}const {table}: [::grpc_core::MethodDescriptor; {}] = [{joined}];", names.len()));
}

fn emit_handler(out: &mut String, service: &Service) {
    put(out, &format!("{}trait {} {{", visibility(service), service.name));
    for method in &service.methods {
        emit_signature(out, method, false, false);
    }
    put(out, "}");
}

fn emit_channel(out: &mut String, service: &Service) {
    put(out, &format!("{}trait {}Channel<C> {{", visibility(service), service.name));
    for method in &service.methods {
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
    if service.methods.is_empty() {
        return;
    }
    let client = format!("{}Client", service.name);
    let channel = format!("{}Channel", service.name);
    put(out, &format!("impl<T, C> {client}<T, C> where T: {channel}<C> {{"));
    for method in &service.methods {
        emit_signature(out, method, true, true);
        let constant = const_name(&service.name, &method.name);
        put(out, &format!("        {channel}::{}(", method.name));
        put(out, "            &mut self.transport,");
        put(out, "            &mut self.codec,");
        put(out, &format!("            &{constant},"));
        put(out, "            &self.compression,");
        put(out, "            request,");
        put(out, "        );");
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
    let table = descriptor_table(name);
    put(out, &format!("impl<S, C> {module}::ServiceHandler for {server}<S, C>"));
    put(out, "where");
    put(out, &format!("    S: {name},"));
    put(out, "{");
    put(out, "    fn service_name(&self) -> &'static str {");
    put(out, &format!("        \"{qualified}\""));
    put(out, "    }");
    put(out, "    fn descriptors(&self) -> &'static [::grpc_core::MethodDescriptor] {");
    put(out, &format!("        &{table}"));
    put(out, "    }");
    put(out, "}");
    if service.methods.is_empty() {
        return;
    }
    put(out, &format!("impl<S, C> {server}<S, C> where S: {name} {{"));
    for method in &service.methods {
        emit_signature(out, method, true, true);
        put(out, &format!("        self.handler.{}(request);", method.name));
        put(out, "    }");
    }
    put(out, "}");
}

fn emit_shell(out: &mut String, service: &Service, server: bool) {
    let vis = visibility(service);
    let module = support_module(service);
    let (ty, param, field) = if server {
        (format!("{}Server", service.name), "S", "handler")
    } else {
        (format!("{}Client", service.name), "T", "transport")
    };
    put(out, &format!("{vis}struct {ty}<{param}, C = {module}::DefaultCodec> {{"));
    put(out, &format!("    {field}: {param},"));
    put(out, "    codec: C,");
    put(out, &format!("    compression: {OPT}<{COMP}>,"));
    put(out, "}");
    put(out, &format!("impl<{param}, C> {ty}<{param}, C> {{"));
    put(out, &format!("    pub fn new({field}: {param}, codec: C) -> Self {{"));
    put(out, &format!("        Self {{ {field}, codec, compression: {OPT}::None }}"));
    put(out, "    }");
    put(out, &format!("    pub fn with_codec<NewCodec>(self) -> {ty}<{param}, NewCodec>"));
    put(out, "    where");
    put(out, "        NewCodec: ::core::default::Default,");
    put(out, "    {");
    put(out, &format!("        {ty} {{"));
    put(out, &format!("            {field}: self.{field},"));
    put(out, "            codec: ::core::default::Default::default(),");
    put(out, "            compression: self.compression,");
    put(out, "        }");
    put(out, "    }");
    put(out, &format!("    pub fn with_compression(mut self, algorithm: {COMP}) -> Self {{"));
    put(out, &format!("        self.compression = {OPT}::Some(algorithm);"));
    put(out, "        self");
    put(out, "    }");
    put(out, "    pub fn codec(&self) -> &C {");
    put(out, "        &self.codec");
    put(out, "    }");
    put(out, &format!("    pub fn {field}(&self) -> &{param} {{"));
    put(out, &format!("        &self.{field}"));
    put(out, "    }");
    put(out, &format!("    pub fn compression(&self) -> {OPT}<&{COMP}> {{"));
    put(out, "        self.compression.as_ref()");
    put(out, "    }");
    put(out, "}");
}

fn emit_signature(out: &mut String, method: &Method, public: bool, body: bool) {
    let prefix = if public { "pub " } else { "" };
    let tail = if body { "{" } else { ";" };
    let request = request_ty(method);
    let response = response_ty(method);
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
    if stream {
        format!("::grpc_core::{wrapper}<::std::vec::Vec<{inner}>>")
    } else {
        format!("::grpc_core::{wrapper}<{inner}>")
    }
}

fn put(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

fn visibility(service: &Service) -> &'static str {
    if service.is_pub { "pub " } else { "" }
}

fn support_module(service: &Service) -> String {
    format!("{}_grpc_support", reshape(&service.name, false))
}

fn const_name(service: &str, method: &str) -> String {
    format!("{}_{}", reshape(service, true), reshape(method, true))
}

fn descriptor_table(service: &str) -> String {
    format!("{}_METHOD_DESCRIPTORS", reshape(service, true))
}

fn qualified_service(package: &str, service: &str) -> String {
    if package.is_empty() { service.to_string() } else { format!("{package}.{service}") }
}

fn method_path(package: &str, service: &str, method: &str) -> String {
    format!("/{}/{}", qualified_service(package, service), rpc_ident(method))
}

fn rpc_ident(name: &str) -> String {
    if name.contains('_') || name.chars().all(|ch| !ch.is_ascii_uppercase()) {
        let mut out = String::new();
        for part in name.split('_').filter(|part| !part.is_empty()) {
            out.push_str(&capitalize(part));
        }
        out
    } else {
        name.to_string()
    }
}

fn capitalize(part: &str) -> String {
    let mut chars = part.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::new();
    out.extend(first.to_uppercase());
    out.push_str(chars.as_str());
    out
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

fn ensure_ident(name: &str, role: &str) -> Result<(), String> {
    if is_rust_ident(name) && !is_keyword(name) {
        Ok(())
    } else {
        Err(format!("{role} `{name}` is not a usable identifier"))
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
    fn pascal_cases_snake_methods() {
        let output = expand_source(r#"package = "demo.v1""#, "pub trait Echo { fn say_hello(request: demo::Ping) -> demo::Pong; }");
        assert_has(&output, "\"/demo.v1.Echo/SayHello\"");
        assert_has(&output, "ECHO_SAY_HELLO");
        assert!(!output.contains("compile_error"), "{output}");
    }

    #[test]
    fn unary_messages_are_not_vectors() {
        let output = expand_source("", "trait Echo { fn unary(Ping) -> Pong; }");
        assert_has(&output, "\"/Echo/Unary\"");
        assert!(!output.contains("Vec"), "{output}");
        assert!(!output.contains("pub struct EchoClient"), "{output}");
    }

    #[test]
    fn merges_stream_directions_and_reports_parse_errors() {
        let output = expand_source("", "trait Echo { #[client_streaming] #[server_streaming] fn chat(Ping) -> Pong; }");
        assert_has(&output, "BidiStreaming");
        assert_has(&output, "\"/Echo/Chat\"");
        let conflict = expand_source("", "trait Echo { #[bidi_streaming] #[server_streaming] fn chat(Ping) -> Pong; }");
        assert_has(&conflict, "compile_error");
        assert_has(&expand_source("", "fn not_a_trait() {}"), "compile_error");
    }
}
