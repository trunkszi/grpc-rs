#![allow(non_snake_case)]

#[grpc::macros::grpc_service]
#[doc = concat! { "Service", " documentation" }]
pub trait Boundary {
    fn Call(request: u32) -> u32;
}

#[test]
fn trait_attributes_preserve_rpc_descriptor() {
    assert_eq!(BOUNDARY_CALL.path, "/Boundary/Call");
    assert_eq!(BOUNDARY_CALL.cardinality, grpc::core::RpcCardinality::Unary);
}
