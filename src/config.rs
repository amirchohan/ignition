//! Startup configuration loading and validation.
//!
//! Config parsing is intentionally outside hot paths. Runtime code should pass
//! the validated numeric `InstrumentSpec` into protocol normalization and core
//! components rather than reading YAML inside matching/risk logic.

use std::{fs, path::Path};

use serde::Deserialize;

use crate::domain::{DomainError, DomainResult, rd::InstrumentSpec};

/// Server configuration loaded from `configs/server.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServerConfig {
    pub reference_data: ReferenceDataConfig,
}

impl ServerConfig {
    /// Load and validate server configuration from disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(ConfigError::Read)?;
        let config = serde_yaml::from_str::<Self>(&text).map_err(ConfigError::Parse)?;
        config.validate().map_err(ConfigError::Validate)?;
        Ok(config)
    }

    /// Validate this parsed config into core-ready reference data.
    pub fn validate(&self) -> DomainResult<InstrumentSpec> {
        self.reference_data.instrument.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReferenceDataConfig {
    pub instrument: InstrumentConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct InstrumentConfig {
    pub instrument_id: u16,
    pub base_asset_id: u16,
    pub quote_asset_id: u16,
    pub price_decimals: u8,
    pub qty_decimals: u8,
    pub price_tick: u64,
    pub qty_lot: u64,
}

impl InstrumentConfig {
    pub fn validate(self) -> DomainResult<InstrumentSpec> {
        InstrumentSpec::new(
            self.instrument_id,
            self.base_asset_id,
            self.quote_asset_id,
            self.price_decimals,
            self.qty_decimals,
            self.price_tick,
            self.qty_lot,
        )
    }
}

/// Config load/parse/validation errors.
#[derive(Debug)]
pub enum ConfigError {
    Read(std::io::Error),
    Parse(serde_yaml::Error),
    Validate(DomainError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> ServerConfig {
        serde_yaml::from_str(input).expect("config should parse")
    }

    #[test]
    fn parses_and_validates_reference_data() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#,
        );

        let instrument = config.validate().expect("valid instrument");
        assert_eq!(instrument.instrument_id.raw(), 1001);
        assert_eq!(instrument.base_asset_id.raw(), 1);
        assert_eq!(instrument.quote_asset_id.raw(), 2);
        assert_eq!(instrument.price_decimals, 2);
        assert_eq!(instrument.qty_decimals, 8);
        assert_eq!(instrument.price_tick, 1);
        assert_eq!(instrument.qty_lot, 1);
    }

    #[test]
    fn rejects_invalid_reference_data() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 1
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#,
        );

        assert_eq!(config.validate(), Err(DomainError::InvalidInstrumentSpec));
    }

    #[test]
    fn rejects_reference_data_ids_that_exceed_u16() {
        let error = serde_yaml::from_str::<ServerConfig>(
            r#"
reference_data:
  instrument:
    instrument_id: 65536
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#,
        )
        .expect_err("instrument id should exceed u16");

        assert!(error.to_string().contains("invalid value"));
    }
}
