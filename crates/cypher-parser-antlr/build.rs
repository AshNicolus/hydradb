use std::{env, error::Error, fs, io, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("set by Cargo"));
    let source_grammar_dir = manifest_dir.join("grammar");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("set by Cargo"));
    let grammar_dir = out_dir.join("grammar");
    let generated_dir = out_dir.join("generated");

    fs::create_dir_all(&grammar_dir)?;
    fs::create_dir_all(&generated_dir)?;

    let lexer_path = source_grammar_dir.join("Cypher25Lexer.g4");
    let parser_path = source_grammar_dir.join("Cypher25Parser.g4");
    println!("cargo:rerun-if-changed={}", lexer_path.display());
    println!("cargo:rerun-if-changed={}", parser_path.display());

    // `METADATA` is both a Neo4j keyword token and the name of ophi's emitted
    // `GrammarMetadata` static. Rename the staged token, not its matched text,
    // to keep those Rust values out of the same namespace.
    let lexer_grammar = fs::read_to_string(&lexer_path)?.replace("METADATA", "METADATA_KW");
    fs::write(grammar_dir.join("Cypher25Lexer.g4"), lexer_grammar)?;

    // Neo4j's grammar carries semantic type hints as ANTLR rule arguments,
    // for example `parameter["INTEGER"]`. They do not change recognition and
    // are consumed only by Neo4j's later AST builder. ophi's Rust generator
    // intentionally accepts a smaller, target-independent argument subset, so
    // strip exactly these unused hints in the generated input. The vendored
    // source grammar remains byte-for-byte untouched.
    let mut parser_grammar = fs::read_to_string(&parser_path)?.replace("METADATA", "METADATA_KW");
    for (from, to) in [
        ("parameter[String paramType]", "parameter"),
        ("parameterName[String paramType]", "parameterName"),
        ("parameterName[paramType]", "parameterName"),
        ("parameter[\"ANY\"]", "parameter"),
        ("parameter[\"INTEGER\"]", "parameter"),
        ("parameter[\"STRING\"]", "parameter"),
        ("parameter[\"MAP\"]", "parameter"),
    ] {
        parser_grammar = parser_grammar.replace(from, to);
    }
    if parser_grammar.contains("paramType") || parser_grammar.contains("parameter[\"") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cypher grammar added an unsupported parameter type hint; update the explicit sanitizer",
        )
        .into());
    }
    fs::write(grammar_dir.join("Cypher25Parser.g4"), parser_grammar)?;

    let generation = antlr_rust_codegen::Builder::new()
        .grammar(grammar_dir.join("Cypher25Lexer.g4"))
        .grammar(grammar_dir.join("Cypher25Parser.g4"))
        .library_directory(&grammar_dir)
        .out_dir(generated_dir)
        .generate()?;

    generation.emit_rerun_if_changed();
    Ok(())
}
