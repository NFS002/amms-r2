use alloy::transports::TransportErrorKind;
use thiserror::Error;

use crate::amms::error::{AMMError, ReorgError};
use alloy_sol_types::Error as AlloyError;

#[derive(Error, Debug)]
pub enum StateSpaceError {
    #[error(transparent)]
    AMMError(#[from] AMMError),
    #[error(transparent)]
    TransportError(#[from] alloy::transports::RpcError<TransportErrorKind>),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Block Number Does not Exist")]
    MissingBlockNumber,
    #[error(transparent)]
    ReOrgError(#[from] ReorgError),
    #[error("Missing block at idx={0}")]
    MissingBlockAtIdx(usize),
    #[error(transparent)]
    AlloyError(#[from] AlloyError),
}
