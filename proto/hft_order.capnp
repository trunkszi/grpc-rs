@0xf9a8361ca37e53d5;
struct Rust { annotation parentModule @0xabee386cd1450364 (file) :Text; }
$Rust.parentModule("hft");

# Streaming annotations describe gRPC cardinality, not Cap'n Proto RPC transport.
annotation clientStreaming(method) :Void;
annotation serverStreaming(method) :Void;
struct OrderRequest { clientOrderId @0 :UInt64; instrumentId @1 :UInt64; quantity @2 :UInt64; priceTicks @3 :Int64; }
struct OrderAck { clientOrderId @0 :UInt64; accepted @1 :Bool; quantity @2 :UInt64; }
struct DepthRequest { instrumentId @0 :UInt64; levels @1 :UInt32; }
struct DepthLevel { priceTicks @0 :Int64; quantity @1 :UInt64; level @2 :UInt32; }
struct FillReport { clientOrderId @0 :UInt64; quantity @1 :UInt64; }
struct FillSummary { totalQuantity @0 :UInt64; fillCount @1 :UInt32; }
interface OrderService {
  placeOrder @0 (request :OrderRequest) -> (response :OrderAck);
  subscribeDepth @1 (request :DepthRequest) -> (response :DepthLevel) $serverStreaming;
  uploadFills @2 (request :FillReport) -> (response :FillSummary) $clientStreaming;
  orderStream @3 (request :OrderRequest) -> (response :OrderAck) $clientStreaming $serverStreaming;
}
