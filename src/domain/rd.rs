pub use super::{Price, Qty, RawNotional};

use super::{DomainError, DomainResult};

/// Numeric instrument reference data consumed by core logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentSpec {
    pub instrument_id: InstrumentId,
    pub base_asset_id: AssetId,
    pub quote_asset_id: AssetId,
    pub price_decimals: u8,
    pub qty_decimals: u8,
    pub price_tick: u64,
    pub qty_lot: u64,
}

impl InstrumentSpec {
    /// Construct validated single-instrument reference data.
    pub fn new(
        instrument_id: u16,
        base_asset_id: u16,
        quote_asset_id: u16,
        price_decimals: u8,
        qty_decimals: u8,
        price_tick: u64,
        qty_lot: u64,
    ) -> DomainResult<Self> {
        if instrument_id == 0 || base_asset_id == 0 || quote_asset_id == 0 {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        let instrument_id = InstrumentId::new(instrument_id)?;
        let base_asset_id = AssetId::new(base_asset_id)?;
        let quote_asset_id = AssetId::new(quote_asset_id)?;

        if base_asset_id == quote_asset_id {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        if price_decimals > MAX_DECIMALS || qty_decimals > MAX_DECIMALS {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        if price_tick == 0 || qty_lot == 0 {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        Ok(Self {
            instrument_id,
            base_asset_id,
            quote_asset_id,
            price_decimals,
            qty_decimals,
            price_tick,
            qty_lot,
        })
    }
}

/// Decimal fields are scaling metadata only; keep bounds small enough for
/// future powers-of-ten display/risk conversion without overflowing `u64`.
pub const MAX_DECIMALS: u8 = 18;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u16);

        impl $name {
            pub const fn raw(self) -> u16 {
                self.0
            }

            pub const fn new(value: u16) -> DomainResult<Self> {
                Ok(Self(value))
            }
        }
    };
}

id_type!(InstrumentId);
id_type!(AssetId);

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 5, 10).expect("valid instrument")
    }

    #[test]
    fn instrument_rejects_invalid_reference_data() {
        assert_eq!(InstrumentSpec::new(0, 1, 2, 2, 8, 1, 1), Err(DomainError::InvalidInstrumentSpec));
        assert_eq!(InstrumentSpec::new(1001, 1, 1, 2, 8, 1, 1), Err(DomainError::InvalidInstrumentSpec));
        assert_eq!(
            InstrumentSpec::new(1001, 1, 2, 19, 8, 1, 1),
            Err(DomainError::InvalidInstrumentSpec)
        );
        assert_eq!(InstrumentSpec::new(1001, 1, 2, 2, 8, 0, 1), Err(DomainError::InvalidInstrumentSpec));
        assert_eq!(InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 0), Err(DomainError::InvalidInstrumentSpec));
    }

    #[test]
    fn price_and_qty_enforce_tick_and_lot() {
        let instrument = instrument();

        assert_eq!(Price::new(10, &instrument).expect("price").raw(), 10);
        assert_eq!(Price::new(11, &instrument), Err(DomainError::InvalidPrice));
        assert_eq!(Price::new(0, &instrument), Err(DomainError::InvalidPrice));

        assert_eq!(Qty::new(20, &instrument).expect("qty").raw(), 20);
        assert_eq!(Qty::new(21, &instrument), Err(DomainError::InvalidQuantity));
        assert_eq!(Qty::new(0, &instrument), Err(DomainError::InvalidQuantity));
    }

    #[test]
    fn raw_notional_uses_wide_integer_math() {
        let instrument = instrument();
        let price = Price::new(u64::MAX, &instrument).expect("price");
        let qty = Qty::new(u64::MAX - 5, &instrument).expect("qty");

        assert_eq!(
            RawNotional::from_price_qty(price, qty).raw(),
            u128::from(u64::MAX) * u128::from(u64::MAX - 5)
        );
    }
}
