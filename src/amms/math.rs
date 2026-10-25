use alloy::primitives::{I256, U256, U512};

use rug::Float;

use super::{
    consts::{MPFR_T_PRECISION, U128_0X10000000000000000},
    error::AMMError,
};

pub fn q64_to_float(num: u128) -> Result<f64, AMMError> {
    let float_num = u128_to_float(num)?;
    let divisor = u128_to_float(U128_0X10000000000000000)?;
    Ok((float_num / divisor).to_f64())
}

pub fn u128_to_float(num: u128) -> Result<Float, AMMError> {
    let value_string = num.to_string();
    let parsed_value = Float::parse_radix(value_string, 10)?;
    Ok(Float::with_val(MPFR_T_PRECISION, parsed_value))
}

pub fn u256_to_float(num: U256) -> Result<Float, AMMError> {
    let value_string = num.to_string();
    let parsed_value = Float::parse_radix(value_string, 10)?;
    Ok(Float::with_val(MPFR_T_PRECISION, parsed_value))
}

/// Signed percentage change in basis points (1 bp = 0.01%), truncated toward zero.
/// Returns an error for zero input or a result outside the I256 range.
pub fn percentage_change_bp(old: U256, new: U256) -> Result<I256, AMMError> {
    if old.is_zero() {
        return Err(AMMError::UndefinedPercentageChange);
    }
    let negative = new < old;
    let diff = if negative { old - new } else { new - old };
    // The full product fits in 512 bits even for U256::MAX.
    let scaled = U512::from(diff) * U512::from(10_000u64) / U512::from(old);
    if scaled > U512::from(I256::MAX.into_raw()) {
        return Err(AMMError::PercentageChangeOverflow);
    }
    let pct_change_bp = I256::from_raw(U256::from(scaled));
    // Losses cannot exceed 10,000 bp because both inputs are unsigned.
    Ok(if negative { -pct_change_bp } else { pct_change_bp })
}

pub fn format_percent_bp(bp: &I256) -> String {
    let sign = if bp.is_negative() { "-" } else { "" };
    let abs = bp.unsigned_abs();

    let integer = abs / U256::from(100);
    let fractional = abs % U256::from(100);

    format!("{sign}{integer}.{fractional:02}%")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentage_examples_and_truncation() {
        for (old, new, expected) in [
            (10000u64, 10001u64, "0.01%"),
            (10000, 9999, "-0.01%"),
            (100, 527, "427.00%"),
            (100, 0, "-100.00%"),
            (100, 100, "0.00%"),
            (30000, 30001, "0.00%"),
            (30000, 29999, "0.00%"),
            (3, 4, "33.33%"),
            (3, 2, "-33.33%"),
        ] {
            let bp = percentage_change_bp(U256::from(old), U256::from(new)).unwrap();
            assert_eq!(format_percent_bp(&bp), expected);
        }
    }

    #[test]
    fn full_width_inputs_preserve_representable_results() {
        let half = U256::MAX / U256::from(2);
        assert_eq!(
            percentage_change_bp(half, half * U256::from(2)).unwrap(),
            I256::unchecked_from(10000)
        );
        assert_eq!(
            percentage_change_bp(U256::MAX, U256::ZERO).unwrap(),
            I256::unchecked_from(-10000)
        );
        assert_eq!(
            percentage_change_bp(U256::MAX, U256::MAX).unwrap(),
            I256::ZERO
        );
    }

    #[test]
    fn invalid_percentages_are_errors() {
        for new in [U256::ZERO, U256::ONE] {
            assert!(matches!(
                percentage_change_bp(U256::ZERO, new),
                Err(AMMError::UndefinedPercentageChange)
            ));
        }
        assert!(matches!(
            percentage_change_bp(U256::ONE, U256::MAX),
            Err(AMMError::PercentageChangeOverflow)
        ));
        // The product fits U256, but must not be reinterpreted as a negative I256.
        let new = (U256::ONE << 255) / U256::from(10000) + U256::from(2);
        assert!(matches!(
            percentage_change_bp(U256::ONE, new),
            Err(AMMError::PercentageChangeOverflow)
        ));
    }

    #[test]
    fn formats_minimum_signed_value_without_overflow() {
        assert_eq!(
            format_percent_bp(&I256::MIN),
            "-578960446186580977117854925043439539266349923328202820197287920039565648199.68%"
        );
    }
}
