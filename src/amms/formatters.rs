pub mod debug_formatters {
    use std::fmt;

    use crate::state_space::BlockRef;

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

    fn debug_block_ref(block_ref: &BlockRef, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let h = short_str(block_ref.hash);
        let ph = short_str(block_ref.parent_hash);
        writeln!(f, "BlockRef ({}): {} -> {}", block_ref.number, h, ph)?;
        let length = block_ref.block_diff.iter().len();
        writeln!(f, "Pools affected: {}", length)?;
        if block_ref.block_diff.is_some() && f.alternate() {
            let first_pools = block_ref.block_diff.as_ref().unwrap().into_iter().take(3);
            for p in first_pools {
                writeln!(f, "{}", p)?;
            }
        }
        f.write_str("\n")
    }
}
