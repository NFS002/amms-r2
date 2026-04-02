pub mod debug_formatters {
    use std::fmt;

    use crate::state_space::{BlockBuffer, BlockRef};

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

    pub fn dbg_block_ref(block_ref: &BlockRef, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let h = short_str(block_ref.hash);
        let ph = short_str(block_ref.parent_hash);
        writeln!(f, "BlockRef ({}): {} -> {}", block_ref.number, ph, h)?;
        let length = block_ref.block_diff.as_ref().map(|d| d.len()).unwrap_or(0);
        writeln!(f, "Pools affected: {}", length)?;
        if f.alternate() {
            if let Some(diff) = &block_ref.block_diff {
                for p in diff.iter().take(3) {
                    fmt_prefix(f, p, "\t")?;
                }
            }
        }
        f.write_str("\n")
    }

    pub fn fmt_prefix<T: std::fmt::Debug>(
        f: &mut fmt::Formatter<'_>,
        value: &T,
        prefix: &str,
    ) -> fmt::Result {
        let s = format!("{:?}", value);
        for line in s.lines() {
            writeln!(f, "{}{}", prefix, line)?;
        }
        Ok(())
    }
}
