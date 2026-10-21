# AGENTS.md

grpc-rs 项目指南。本文件是代理在本仓库工作的操作契约；`CLAUDE.md` 只导入本文件。

## 项目定位

grpc-rs 是基于 nghttp2-rs 的 Rust 2024 同步 gRPC 框架，支持 Protobuf、Apache Fory、Cap'n Proto 及自定义 codec。连接、协议会话和流句柄由调用线程推进，通过传输就绪与截止时间驱动进展，不需要执行器或后台运行时。

## 当前工作区

```text
grpc-rs/
  Cargo.toml                         # 成员、依赖来源、lint 与 profile
  Cargo.lock                         # 已验证的依赖版本与 Git 提交
  rust-toolchain.toml                 # nightly 工具链
  proto/hft_order.proto              # Protobuf HFT 消息与服务
  proto/hft_order.fdl                # Apache Fory HFT 消息与服务
  proto/hft_order.capnp              # Cap'n Proto HFT 消息与流式注解
  crates/grpc-core/                  # 帧、压缩、元数据、会话与同步传输
  crates/grpc-codec/prost/            # Protobuf codec
  crates/grpc-codec/fory/             # Apache Fory codec
  crates/grpc-codec/capnp/            # Cap'n Proto codec
  crates/grpc-macros/                # 同步服务属性宏
  crates/grpc-build/                 # IDL 编译与服务生成
  crates/grpc/                       # Channel、Server、流接口及 build.rs
    examples/hft_order_service.rs    # 三种 codec、四种 RPC 形态的 TCP 示例
    benches/hft_rpc_bench.rs         # 结构化消息的四形态 RPC 批次基准
    tests/                          # 生命周期、协议异常与端到端验证
  .github/                          # CI、工具链与隔离门禁
```

根配置管理共享依赖、lint 和 profile。已提交 IDL 是消息与服务定义的权威来源；生成的 Rust 文件保存在 `OUT_DIR`。

## 包分层

| 层级 | 包 | 职责与依赖边界 |
|---|---|---|
| 协议核心 | `grpc-core` | 依赖 nghttp2-rs，提供同步传输和协议状态机；不依赖 codec 适配器 |
| 序列化 | `grpc-codec-*` | 依赖 grpc-core 及对应序列化后端 |
| 服务宏 | `grpc-macros` | 生成同步服务接口；展开代码引用 grpc-core |
| IDL 构建 | `grpc-build` | 调用外部 IDL 编译器及按 feature 启用的 Rust 消息生成器 |
| 应用接口 | `grpc` | 装配核心、宏和所选 codec；构建时使用 grpc-build |

## Feature

| 包 / Feature | 行为 |
|---|---|
| grpc / protobuf（默认） | Prost codec 与 Protobuf IDL 生成 |
| grpc / fory | Fory codec 与 foryc 消息生成；独立构建不引入 Prost |
| grpc / capnp | Cap'n Proto codec 与 capnpc Rust 消息生成 |
| grpc-build / protobuf（默认） | 使用指定 protoc，通过 prost-build 生成消息 |
| grpc-build / capnp（默认） | 使用指定 capnp，通过 capnpc 生成 Rust 消息 |

`crates/grpc/build.rs` 只编译启用的 codec 对应 IDL。`SchemaCompiler` 同时生成 `*_grpc.rs` 同步服务接口和方法描述符；Fory 原生 RPC 绑定与本框架的同步绑定分别生成。Cap'n Proto 方法注解表达 gRPC 流式形态。

## 命令

从工作区根目录执行。按所选 codec 安装 protoc、foryc 和 capnp；`PROTOC`、`FORYC` 可覆盖工具发现路径，显式 builder 路径优先。`NGHTTP2_DIR` 可指定原生 nghttp2 源码树，否则使用上游构建规则。

| 命令 | 用途 |
|---|---|
| `cargo fmt --all -- --check` | 格式门禁 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | 全目标、全 feature 严格 lint |
| `cargo test --workspace --all-targets --all-features` | 单元、集成、端到端和基准目标验证 |
| `cargo check -p grpc --no-default-features --features fory` | Fory 独立构建 |
| `cargo run -p grpc --all-features --example hft_order_service` | 三种 codec 的四形态结构化 HFT RPC |
| `cargo bench -p grpc --all-features --bench hft_rpc_bench` | 四形态 RPC 批次延迟 |
| `bash .github/scripts/ci-incremental.sh` | 源码治理、依赖和 codec 组合门禁 |
| `git diff --check` | 差异空白检查 |

代码变更完成前执行格式、lint 和受影响测试；共享接口变化覆盖调用方及 feature 组合。无法运行的验证须说明前置条件，不计为通过。纯文档修改核对路径、命令与实现，无需启动 Rust 构建。

## 硬约束

- 每个 `.rs`、`.c`、`.h` 文件不超过 800 行；按职责拆分实现和测试。
- 所有依赖使用 `{ ... }` 内联表，共享版本从根 `[workspace.dependencies]` 继承；保持根 lint 和 profile 约束。
- 库错误派生 `thiserror::Error`，保留稳定显示文本和 source 链；构建、示例、基准和可失败测试入口使用 `anyhow::Result`。协议失败保留类型化 `grpc-core::Status`。
- 保持同步依赖图，不引入 async 运行时、futures 或异步 RPC 栈。
- 通过 grpc-build 生成 IDL 消息与服务；用真实生成消息验证 codec 和传输边界。
- 每个 unsafe 块附准确的 `// SAFETY:` 注释，公共 unsafe 函数记录 `# Safety`；可失败或会 panic 的公共函数分别记录 `# Errors` / `# Panics`。
- 本指南使用中文；源码注释、rustdoc、错误消息、日志和提交消息使用英文。临时规划资料保持忽略且未跟踪，不暂存被忽略的路径。

## 生命周期与接口

- 连接、会话和流句柄保持其单线程所有权约束；只在明确的应用边界转移传输所有权。
- 回调借用不得越过使其失效的会话操作；连接关闭前保留回调和发送缓冲区资源。
- 帧、解压、流队列和超时预算保持有界；畸形输入返回具名错误。
- 关闭时保留已完成的接收消息；排空响应 DATA 与 trailer 后再终止相应发送方向。
- 修改状态转换时覆盖重复、晚到、取消、超时和资源释放，不以进程存活代替业务断言。

## 工作范围与维护

修改前检查已暂存与未暂存差异，保留其他工作；仅按用户授权提交或推送，不擅自合并或执行破坏性 Git 操作。

完成前核对最终变更范围、差异空白、源码行数和验证结果。包名、路径、feature、命令或所有权契约变化时同步本文件；`CLAUDE.md` 保持单行导入，不复制规则。
