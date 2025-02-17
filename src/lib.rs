#![doc = include_str!("../README.md")]
pub mod bucket;
pub mod common;
pub mod error;
pub mod oss;
pub mod request;
mod util;

#[cfg(feature = "serde")]
pub use serde;
