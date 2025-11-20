# hyperware-pubsub

Basic structure: publisher - broker - subscriber.

Workspace for Hyperware's general-purpose publish/subscribe infrastructure. The
core abstractions live in this root crate under `src/` and currently provide:

- Transport types + broker traits (`lib.rs`)
- Authentication & ACL primitives (`auth.rs`)
- Routing strategies (`router.rs`) with direct and hub-spoke policies
- Queue storage interfaces + in-memory backend (`storage.rs`)
