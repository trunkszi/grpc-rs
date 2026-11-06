use super::*;

#[test]
fn trait_attributes_may_contain_braces() {
    let expanded = expand_source(
        "",
        r#"
        #[doc = concat! { "Service", " documentation" }]
        pub trait Boundary {
            fn Call(request: u32) -> u32;
        }
    "#,
    );
    assert!(!expanded.contains("compile_error"), "{expanded}");
    assert!(expanded.contains("BoundaryClient"), "{expanded}");
    assert!(expanded.contains("/Boundary/Call"), "{expanded}");
}
