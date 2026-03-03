pub mod debug_formatters {

    pub fn short_str<T: ToString>(b: T) -> String {
        let full_str = b.to_string();
        if full_str.len() <= 7 {
            return full_str;
        };
        let mut iter = full_str.chars();

        let first: String = iter.by_ref().take(3).collect();

        let last: String = full_str
        .to_string()
            .chars()
            .rev()
            .take(3)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        format!("{}..{}", first, last)
    }
}
