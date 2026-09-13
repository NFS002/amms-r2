pub mod debug_formatters {
    use std::fmt;

    use alloy::primitives::{Address, Keccak256};

    use crate::state_space::{BlockBuffer, BlockRef};

    const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    pub fn base62_encode(mut num: u64) -> String {
        let mut buf = Vec::new();

        for _ in 0..6 {
            buf.push(BASE62[(num % 62) as usize]);
            num /= 62;
        }

        buf.reverse();
        String::from_utf8(buf).unwrap()
    }

    pub fn short_str<T: ToString>(b: T) -> String {
        let full_str = b.to_string();
        if full_str.len() <= 7 {
            return full_str;
        };
        let mut iter = full_str.chars();

        let first: String = iter.by_ref().skip(2).take(3).collect();

        let last: String = iter.by_ref().skip(full_str.len() - 3).take(3).collect();

        format!("{}{}", first, last)
    }

    pub fn dbg_block_ref(block_ref: &BlockRef, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let h = block_ref.hash;
        let ph = block_ref.parent_hash;
        writeln!(f, "BlockRef ({}): {} -> {}", block_ref.number, ph, h)?;
        let length = block_ref.block_diff.as_ref().map(|d| d.len()).unwrap_or(0);
        if f.alternate() {
            writeln!(f, "Pools affected: {}", length)?;
            if let Some(diff) = &block_ref.block_diff {
                for p in diff.iter().take(3) {
                    fmt_prefix(f, p, "\t")?;
                }
            }
        }
        f.write_str("\n")
    }

    pub fn fmt_prefix<T>(f: &mut fmt::Formatter<'_>, value: &T, prefix: &str) -> fmt::Result
    where
        T: std::fmt::Display + std::fmt::Debug,
    {
        let s = if f.alternate() {
            format!("{:#?}", value)
        } else {
            format!("{}", value)
        };
        for line in s.lines() {
            writeln!(f, "{}{}", prefix, line)?;
        }
        Ok(())
    }
}
