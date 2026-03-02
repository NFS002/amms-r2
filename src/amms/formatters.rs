pub mod debug_formatters {
    use alloy::primitives::BlockHash;

    pub fn short_hash(b: &BlockHash) -> String {
        let full_hash = b.to_string();
        let mut iter = full_hash.chars();

        let first: String = iter.by_ref().take(3).collect();

        let last: String = full_hash
        .to_string()
            .chars()
            .rev()
            .take(3)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        format!("{}...{}", first, last)
    }
}
