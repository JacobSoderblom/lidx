use crate::model::{ContextItem, ItemSource, SourceType};

pub(super) fn sort_items(items: &mut [ContextItem]) {
    items.sort_by(|a, b| {
        let source_rank = |source: &ItemSource| -> u8 {
            match source.source_type {
                SourceType::DirectSeed => 0,
                SourceType::Subgraph => 1,
                SourceType::Search => 2,
            }
        };

        source_rank(&a.source)
            .cmp(&source_rank(&b.source))
            .then_with(|| a.source.seed_index.cmp(&b.source.seed_index))
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.start_line.cmp(&b.start_line))
            .then_with(|| a.start_byte.cmp(&b.start_byte))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_items_is_deterministic() {
        let mut items1 = vec![
            ContextItem {
                source: ItemSource {
                    source_type: SourceType::Subgraph,
                    seed_index: None,
                    relationship: Some("related".to_string()),
                    distance: None,
                },
                path: "b.rs".into(),
                start_line: Some(10),
                end_line: Some(10),
                start_byte: 0,
                end_byte: 10,
                content: "test".into(),
                symbol: None,
                score: None,
                match_location: None,
            },
            ContextItem {
                source: ItemSource {
                    source_type: SourceType::DirectSeed,
                    seed_index: Some(0),
                    relationship: None,
                    distance: Some(0),
                },
                path: "a.rs".into(),
                start_line: Some(1),
                end_line: Some(1),
                start_byte: 0,
                end_byte: 10,
                content: "test".into(),
                symbol: None,
                score: None,
                match_location: None,
            },
            ContextItem {
                source: ItemSource {
                    source_type: SourceType::DirectSeed,
                    seed_index: Some(1),
                    relationship: None,
                    distance: Some(0),
                },
                path: "a.rs".into(),
                start_line: Some(5),
                end_line: Some(5),
                start_byte: 0,
                end_byte: 10,
                content: "test".into(),
                symbol: None,
                score: None,
                match_location: None,
            },
        ];

        let mut items2 = items1.clone();
        items2.reverse();

        sort_items(&mut items1);
        sort_items(&mut items2);

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
