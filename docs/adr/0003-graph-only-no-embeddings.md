# Graph-only retrieval, no vector embeddings

Staying with Tree-sitter AST parsing + SQLite symbol graph + text search. No vector embeddings or semantic search despite the market research recommending hybrid.

Embeddings were implemented and stripped in February 2026 (BGE-small scores too compressed, ~3000 lines removed). Graph + text search covers known use cases. User feedback shows 70% of value from search + trace_flow — no demand for semantic similarity.

Structural precision ("we know ProcessRefund calls CreateEntry across a gRPC boundary") is the positioning. Fuzzy similarity dilutes that message and adds complexity without proven demand.

Revisit if a design partner specifically requests semantic search as a condition for adoption.
