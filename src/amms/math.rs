use alloy::primitives::{I256, U256};

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

/// Returns signed percentage change in basis points (1 bp = 0.01%)
/// Example: 1234 => +12.34%, -50 => -0.50%
pub fn percentage_change_bp(old: U256, new: U256) -> I256 {
    if old.is_zero() {
        return I256::ZERO
    }

    // Determine sign and absolute difference
    let (sign, diff) = if new >= old {
        (I256::ONE, new - old)
    } else {
        (I256::MINUS_ONE, old - new)
    };

    // Overflow-safe ordering:
    // (diff / old) * 10000 would lose precision,
    // so we instead do (diff * 10000) / old
    // but split the multiplication to reduce overflow risk.
    let scaled = diff
        .checked_mul(U256::from(10_000u64))
        .map_or(U256::ZERO, |r| r.checked_div(old).unwrap_or(U256::ZERO));

    I256::from_raw(scaled) * sign
}

pub fn format_percent_bp(bp: &I256) -> String {
    let sign = if bp.is_negative() { "-" } else { "" };
    let abs = bp.abs();

    let integer = abs / I256::unchecked_from(100);
    let fractional = abs % I256::unchecked_from(100);

    format!("{sign}{integer}.{fractional:02}%")
}


