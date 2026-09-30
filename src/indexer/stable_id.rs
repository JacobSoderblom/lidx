use crate::indexer::extract::SymbolInput;
use blake3::Hasher;

/// Compute a stable symbol ID from content only (NO LINE NUMBERS).
///
/// This function generates a content-based identifier for a symbol that remains
/// stable across code moves, whitespace changes, and reformatting. The stable ID
/// is computed from:
/// - `qualname`: The fully-qualified name (e.g., "module.Class.method")
/// - `signature`: The function/method signature (parameters, return type)
/// - `kind`: The symbol kind (function, class, variable, etc.)
///
/// Importantly, it does NOT include line numbers or byte positions, which would
/// change when blank lines are added or code is moved.
///
/// # Format
///
/// Returns a string in the format `sym_{16_hex_chars}` where the hex characters
/// are the first 16 characters (64 bits) of the blake3 hash of the symbol content.
///
/// # Examples
///
/// ```ignore
/// // These two symbols have the same stable ID:
/// let sym1 = SymbolInput {
///     qualname: "MyClass.authenticate".to_string(),
///     signature: Some("(username: str, password: str) -> User".to_string()),
///     kind: "function".to_string(),
///     start_line: 10,  // Different line number
///     // ... other fields
///, identity: None, };
///
/// let sym2 = SymbolInput {
///     qualname: "MyClass.authenticate".to_string(),
///     signature: Some("(username: str, password: str) -> User".to_string()),
///     kind: "function".to_string(),
///     start_line: 20,  // Different line number (blank line added)
///     // ... other fields
///, identity: None, };
///
/// assert_eq!(compute_stable_symbol_id(&sym1), compute_stable_symbol_id(&sym2));
/// ```
pub fn compute_stable_symbol_id(symbol: &SymbolInput) -> String {
    let mut hasher = Hasher::new();

    // Include ONLY semantic content that identifies the symbol
    hasher.update(symbol.qualname.as_bytes());
    hasher.update(b"\x00"); // Null byte separator

    // Include signature if present (parameters, return type)
    if let Some(sig) = &symbol.signature {
        hasher.update(sig.as_bytes());
    }
    hasher.update(b"\x00"); // Null byte separator

    // Include kind for disambiguation (function vs class with same name)
    hasher.update(symbol.kind.as_bytes());

    // Declaration identity (generic arity, cfg, impl block, collision
    // ordinal). Absent for the common case, so existing ids are unchanged.
    if let Some(identity) = &symbol.identity {
        hasher.update(b"\x00");
        hasher.update(identity.as_bytes());
    }

    // DO NOT include start_line, end_line, start_byte, end_byte
    // These change when blank lines are added or code is moved!

    let hash = hasher.finalize();
    // Use first 64 bits (16 hex characters) of hash
    format!("sym_{}", &hash.to_hex()[..16])
}

/// One group of symbols that hashed to the same stable id before
/// disambiguation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StableIdCollision {
    pub qualname: String,
    pub kind: String,
    /// How many declarations shared the id (always >= 2).
    pub count: usize,
}

/// Guarantees every symbol in `symbols` (one file's) has a distinct stable
/// id, without ever dropping one. A symbol whose id is already taken by an
/// earlier symbol gets a `dup<N>` ordinal in its `identity` (N = its
/// position among the twins, in declaration order), which is deterministic
/// and independent of line numbers. Returns one entry per colliding group so
/// callers can surface it; empty when every id was already unique.
pub fn disambiguate_collisions(symbols: &mut [SymbolInput]) -> Vec<StableIdCollision> {
    use std::collections::HashMap;
    let mut seen: HashMap<String, usize> = HashMap::with_capacity(symbols.len());
    let mut groups: Vec<StableIdCollision> = Vec::new();
    let mut group_of: HashMap<String, usize> = HashMap::new();
    for symbol in symbols.iter_mut() {
        let base = compute_stable_symbol_id(symbol);
        let mut id = base.clone();
        let mut ordinal = *seen.get(&base).unwrap_or(&0);
        if ordinal > 0 {
            let original = symbol.identity.clone();
            loop {
                let tagged = match &original {
                    Some(existing) => format!("{existing}|dup{ordinal}"),
                    None => format!("dup{ordinal}"),
                };
                symbol.identity = Some(tagged);
                id = compute_stable_symbol_id(symbol);
                if !seen.contains_key(&id) {
                    break;
                }
                ordinal += 1;
            }
            match group_of.get(&base) {
                Some(&g) => groups[g].count += 1,
                None => {
                    group_of.insert(base.clone(), groups.len());
                    groups.push(StableIdCollision {
                        qualname: symbol.qualname.clone(),
                        kind: symbol.kind.clone(),
                        count: 2,
                    });
                }
            }
        }
        *seen.entry(base).or_insert(0) += 1;
        seen.insert(id, 1);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_symbol(
        qualname: &str,
        signature: Option<&str>,
        kind: &str,
        start_line: i64,
    ) -> SymbolInput {
        SymbolInput {
            kind: kind.to_string(),
            name: qualname
                .split('.')
                .next_back()
                .unwrap_or(qualname)
                .to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line: start_line + 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 100,
            signature: signature.map(String::from),
            docstring: None,
            identity: None,
        }
    }

    #[test]
    fn test_stable_id_survives_line_changes() {
        // Same symbol at different line numbers should have the same stable ID
        let sym1 = make_test_symbol(
            "module.MyClass.authenticate",
            Some("(username: str, password: str) -> User"),
            "function",
            10,
        );

        let sym2 = make_test_symbol(
            "module.MyClass.authenticate",
            Some("(username: str, password: str) -> User"),
            "function",
            100, // Different line number
        );

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_eq!(
            id1, id2,
            "Stable IDs should be the same despite different line numbers"
        );
    }

    #[test]
    fn test_stable_id_changes_with_signature() {
        // Same qualname but different signature should have different stable IDs
        let sym1 = make_test_symbol(
            "module.MyClass.authenticate",
            Some("(username: str, password: str) -> User"),
            "function",
            10,
        );

        let sym2 = make_test_symbol(
            "module.MyClass.authenticate",
            Some("(username: str, password: bytes) -> User"), // Different signature
            "function",
            10,
        );

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_ne!(id1, id2, "Stable IDs should differ when signature changes");
    }

    #[test]
    fn test_stable_id_changes_with_qualname() {
        // Different qualnames should have different stable IDs
        let sym1 = make_test_symbol(
            "module.MyClass.authenticate",
            Some("(username: str) -> User"),
            "function",
            10,
        );

        let sym2 = make_test_symbol(
            "module.MyClass.authorize", // Different method name
            Some("(username: str) -> User"),
            "function",
            10,
        );

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_ne!(id1, id2, "Stable IDs should differ when qualname changes");
    }

    #[test]
    fn test_stable_id_changes_with_kind() {
        // Same qualname but different kind should have different stable IDs
        let sym1 = make_test_symbol("module.MyClass", None, "class", 10);

        let sym2 = make_test_symbol("module.MyClass", None, "interface", 10);

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_ne!(id1, id2, "Stable IDs should differ when kind changes");
    }

    #[test]
    fn test_stable_id_format() {
        // Verify the format is "sym_{16_hex_chars}"
        let sym = make_test_symbol("module.test", None, "function", 10);
        let id = compute_stable_symbol_id(&sym);

        assert!(id.starts_with("sym_"), "ID should start with 'sym_'");
        assert_eq!(id.len(), 20, "ID should be 'sym_' + 16 hex chars = 20");

        // Verify the hex part is valid hexadecimal
        let hex_part = &id[4..];
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "ID suffix should be hexadecimal"
        );
    }

    #[test]
    fn test_stable_id_no_signature() {
        // Symbols without signatures should still work
        let sym1 = make_test_symbol("module.CONSTANT", None, "variable", 10);
        let sym2 = make_test_symbol("module.CONSTANT", None, "variable", 50);

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_eq!(
            id1, id2,
            "Symbols without signatures should have stable IDs"
        );
    }

    #[test]
    fn test_stable_id_deterministic() {
        // Computing the same symbol multiple times should give the same ID
        let sym = make_test_symbol(
            "module.MyClass.method",
            Some("(arg: int) -> bool"),
            "function",
            42,
        );

        let id1 = compute_stable_symbol_id(&sym);
        let id2 = compute_stable_symbol_id(&sym);
        let id3 = compute_stable_symbol_id(&sym);

        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
    }
}
