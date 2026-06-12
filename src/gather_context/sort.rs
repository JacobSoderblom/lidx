use crate::model::{ContextItem, ItemSource, SourceType};

/// Sort items deterministically for consistent output
pub(super) fn sort_items(items: &mut [ContextItem]) {
    items.sort_by(|a, b| {
        // Primary: source type (seeds before subgraph)
        let source_rank = |source: &ItemSource| -> u8 {
            match source.source_type {
                SourceType::DirectSeed => 0,
                SourceType::Subgraph => 1,
                SourceType::Search => 2,
            }
        };

        source_rank(&a.source)
            .cmp(&source_rank(&b.source))
            // Secondary: seed index (if both are direct seeds)
            .then_with(|| a.source.seed_index.cmp(&b.source.seed_index))
            // Tertiary: path (alphabetical)
            .then_with(|| a.path.cmp(&b.path))
            // Fourth: start line
            .then_with(|| a.start_line.cmp(&b.start_line))
            // Finally: start byte for regions within same line
            .then_with(|| a.start_byte.cmp(&b.start_byte))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(
        source_type: SourceType,
        seed_index: Option<usize>,
        path: &str,
        line: i64,
    ) -> ContextItem {
        ContextItem {
            source: ItemSource {
                source_type,
                seed_index,
                relationship: None,
                distance: None,
            },
            path: path.into(),
            start_line: Some(line),
            end_line: Some(line),
            start_byte: 0,
            end_byte: 10,
            content: "test".into(),
            symbol: None,
            score: None,
            match_location: None,
        }
    }

    #[test]
    fn sort_items_is_deterministic() {
        let mut items1 = vec![
            item(SourceType::Subgraph, None, "b.rs", 10),
            item(SourceType::DirectSeed, Some(0), "a.rs", 1),
            item(SourceType::DirectSeed, Some(1), "a.rs", 5),
        ];

        let mut items2 = items1.clone();
        items2.reverse();

        sort_items(&mut items1);
        sort_items(&mut items2);

        // Both should have same order after sorting
        assert_eq!(items1[0].path, "a.rs");
        assert_eq!(items1[0].start_line, Some(1));
        assert_eq!(items1[1].start_line, Some(5));
        assert!(matches!(items1[2].source.source_type, SourceType::Subgraph));

        for (a, b) in items1.iter().zip(items2.iter()) {
            assert_eq!(a.path, b.path);
            assert_eq!(a.start_line, b.start_line);
        }
    }
}
