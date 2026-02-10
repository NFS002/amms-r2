
use std::fs::{exists as file_exists};

use crate::amms::error::{AMMError, IOError};


pub fn amms_file_exists(input_file: &str) -> Result<(), AMMError> {
    let exists = file_exists(input_file)
        .map_err(|e| AMMError::IOError(IOError::InvalidPath))?;

    if !exists {
        return Err(AMMError::IOError(IOError::FileNotFound));
    }

    Ok(())
}