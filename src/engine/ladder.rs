//! Paged price-level storage and fast best-price discovery for one book side.
//!
//! A `SideLadder` represents either all bids or all asks inside the configured
//! price range. It translates an integer price into a logical page and an
//! offset within that page, records which prices are occupied, and finds the
//! next or best occupied price without scanning every possible price.
//!
//! The configured price range may be much wider than the number of price pages
//! that are active at one time. The ladder therefore separates logical pages
//! from physical storage: `page_slots` covers the full price range, while
//! `PagePool` contains only the bounded number of preallocated pages that may
//! hold live levels. Activating and releasing a logical page does not allocate.
//!
//! # Main components
//!
//! - [`SideLadder`] ties the mapping, page pool, page-level occupancy bitmap,
//!   and cached best price together for one side of the book.
//! - [`PriceLevel`] stores aggregate quantity, order count, and the FIFO
//!   endpoints for one price. Actual resting orders live in `OrderArena`.
//! - [`LevelKey`] is the translated address of one price: logical page index,
//!   offset within that page, and the original integer price.
//! - `Bitmap` stores occupancy as packed bits and can jump to the next or
//!   previous set bit using integer bit operations.
//! - `Page` is a reusable dense group of price levels with its own level
//!   occupancy bitmap.
//! - `PagePool` owns every physical page available to one side and a freelist
//!   of page handles that are not currently assigned to a logical price range.
//!
//! # How lookups work
//!
//! A price is first converted to a tick offset from the configured minimum.
//! Division by `page_size` selects its logical page and the remainder selects
//! its level within that page. `page_slots` then maps the logical page to a
//! physical page-pool handle. Empty logical pages have no physical page.
//!
//! There are two occupancy layers. `occupied_pages` identifies logical pages
//! containing visible quantity, and each active `Page` identifies its occupied
//! levels. Traversal checks the current page first, then uses the outer bitmap
//! to jump directly to the next active page.
//!
//! # Important invariants
//!
//! A level is occupied exactly when it contains visible resting orders. A
//! logical page is occupied exactly when at least one of its levels is occupied.
//! An empty page is reset before its physical storage returns to the pool. The
//! cached `best` key is either absent or points to an occupied level; bids choose
//! the highest occupied price and asks choose the lowest.
//!
//! `book.rs` updates level aggregates and calls `mark_occupied` or `mark_empty`.
//! `matching.rs` reads best and neighboring levels. `capacity.rs` asks whether
//! a page can be activated or deterministically released before mutation.

use crate::domain::{DomainError, DomainResult, Price, trading::order::Side};

use super::{EngineConfig, orders::OrderHandle};

/// One book side backed by a bounded pool of paged price levels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SideLadder {
    /// Buy or sell ordering rule represented by this ladder.
    side: Side,
    /// Lowest integer price that can be translated into this ladder.
    storage_min_price: Price,
    /// Highest integer price that can be translated into this ladder.
    storage_max_price: Price,
    /// Tick size in integer price units.
    tick_size: u64,
    /// Number of adjacent tick prices stored in each physical page.
    page_size: usize,
    /// Logical price-page to physical page-pool handle mapping.
    page_slots: Vec<Option<usize>>,
    /// Bits for logical pages containing at least one occupied level.
    occupied_pages: Bitmap,
    /// Preallocated physical page storage and its freelist.
    page_pool: PagePool,
    /// Cached highest bid or lowest ask, depending on `side`.
    best: Option<LevelKey>,
}

impl SideLadder {
    /// Create an empty side ladder from validated engine config.
    pub(crate) fn new(side: Side, config: EngineConfig) -> DomainResult<Self> {
        // inclusive tick span determines how many logical levels the configured
        // storage envelope can address.
        let tick_span = (config.storage_max_price().raw() - config.storage_min_price().raw()) / config.instrument().price_tick;
        let page_count = (tick_span as usize + 1).div_ceil(config.page_size() as usize);

        if page_count == 0 {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        Ok(Self {
            side,
            storage_min_price: config.storage_min_price(),
            storage_max_price: config.storage_max_price(),
            tick_size: config.instrument().price_tick,
            page_size: config.page_size() as usize,
            page_slots: vec![None; page_count],
            occupied_pages: Bitmap::new(page_count),
            page_pool: PagePool::new(config.max_active_pages() as usize, config.page_size() as usize),
            best: None,
        })
    }

    /// Translate an in-range price into its logical ladder address.
    pub(crate) fn key_for_price(&self, price: Price) -> DomainResult<LevelKey> {
        if price < self.storage_min_price || price > self.storage_max_price {
            return Err(DomainError::InvalidPrice);
        }

        // prices are already tick-aligned domain values, so integer division
        // maps the offset exactly onto a level.
        let tick = (price.raw() - self.storage_min_price.raw()) / self.tick_size;
        let page_index = tick as usize / self.page_size;
        let offset = tick as usize % self.page_size;
        Ok(LevelKey { page_index, offset, price })
    }

    /// Return a level only if its page is currently active.
    pub(crate) fn get_level(&self, key: LevelKey) -> Option<&PriceLevel> {
        let page_handle = self.page_slots.get(key.page_index).and_then(|slot| *slot)?;
        self.page_pool.pages[page_handle].levels.get(key.offset)
    }

    /// Return whether `key` can be addressed without exhausting the active-page pool.
    pub(crate) fn can_rest_at(&self, key: LevelKey) -> bool {
        // an already active logical page needs no free physical page.
        self.page_slots.get(key.page_index).is_some_and(Option::is_some) || !self.page_pool.free.is_empty()
    }

    /// Return whether removing one order at `key` would release its active page.
    pub(crate) fn page_released_after_removing_order(&self, key: LevelKey) -> bool {
        let Some(page_handle) = self.page_slots.get(key.page_index).and_then(|slot| *slot) else {
            return false;
        };
        let page = &self.page_pool.pages[page_handle];
        // the removed order must empty its level, and that level must be the
        // only occupied position anywhere in the physical page.
        page.levels[key.offset].order_count == 1 && page.occupied.contains_only(key.offset)
    }

    /// Return a mutable level, activating its page from the pool if needed.
    pub(crate) fn get_level_mut_or_activate(&mut self, key: LevelKey) -> DomainResult<&mut PriceLevel> {
        let slot = self.page_slots.get_mut(key.page_index).ok_or(DomainError::Malformed)?;
        if slot.is_none() {
            // page activation only assigns preallocated storage; exhaustion is
            // reported instead of growing the pool.
            let page_handle = self.page_pool.activate().ok_or(DomainError::InvalidInstrumentSpec)?;
            *slot = Some(page_handle);
        }

        let page_handle = slot.ok_or(DomainError::Malformed)?;
        self.page_pool
            .pages
            .get_mut(page_handle)
            .and_then(|page| page.levels.get_mut(key.offset))
            .ok_or(DomainError::Malformed)
    }

    /// Mark a level as visible in the side's occupancy indexes.
    pub(crate) fn mark_occupied(&mut self, key: LevelKey) -> DomainResult<()> {
        let page_handle = self
            .page_slots
            .get(key.page_index)
            .and_then(|slot| *slot)
            .ok_or(DomainError::Malformed)?;
        let page = self.page_pool.pages.get_mut(page_handle).ok_or(DomainError::Malformed)?;
        if key.offset >= page.levels.len() {
            return Err(DomainError::Malformed);
        }
        // update both occupancy layers before exposing the key as best.
        page.occupied.set(key.offset);
        self.occupied_pages.set(key.page_index);
        self.refresh_best_after_occupy(key);
        Ok(())
    }

    /// Mark a level empty and release its page when no levels remain active.
    pub(crate) fn mark_empty(&mut self, key: LevelKey) -> DomainResult<()> {
        let page_handle = self
            .page_slots
            .get(key.page_index)
            .and_then(|slot| *slot)
            .ok_or(DomainError::Malformed)?;

        self.page_pool.pages[page_handle].occupied.clear(key.offset);
        if !self.page_pool.pages[page_handle].occupied.any() {
            // an empty logical page no longer needs scarce physical storage.
            self.occupied_pages.clear(key.page_index);
            self.page_slots[key.page_index] = None;
            self.page_pool.release(page_handle);
        }

        if self.best == Some(key) {
            // only removing the cached best requires a bitmap search; removing
            // any other level leaves the cached key valid.
            self.best = self.find_best();
        }
        Ok(())
    }

    /// Return the best level key and metadata for this side.
    pub(crate) fn best_level(&self) -> DomainResult<Option<(LevelKey, &PriceLevel)>> {
        let Some(key) = self.best else {
            return Ok(None);
        };
        let level = self.get_level(key).ok_or(DomainError::Malformed)?;
        if level.is_empty() {
            return Err(DomainError::Malformed);
        }
        Ok(Some((key, level)))
    }

    /// Return the top price and aggregate quantity for this side.
    pub(crate) fn top(&self) -> DomainResult<Option<(Price, u64)>> {
        Ok(self.best_level()?.map(|(key, level)| (key.price, level.total_qty)))
    }

    /// Return the next occupied key at a lower price than `current`.
    pub(crate) fn occupied_before(&self, current: LevelKey) -> Option<LevelKey> {
        // prefer another occupied level in the same physical page before
        // consulting the outer logical-page bitmap.
        if current.offset > 0
            && let Some(page_handle) = self.page_slots[current.page_index]
            && let Some(offset) = self.page_pool.pages[page_handle].occupied.prev_set_from(current.offset - 1)
        {
            return self.key_from_page_offset(current.page_index, offset);
        }

        let page_index = current.page_index.checked_sub(1)?;
        let page_index = self.occupied_pages.prev_set_from(page_index)?;
        let page_handle = self.page_slots[page_index]?;
        let offset = self.page_pool.pages[page_handle].occupied.prev_set_from(self.page_size - 1)?;
        self.key_from_page_offset(page_index, offset)
    }

    /// Return the next occupied key at a higher price than `current`.
    pub(crate) fn occupied_after(&self, current: LevelKey) -> Option<LevelKey> {
        // prefer another occupied level in the same physical page before
        // consulting the outer logical-page bitmap.
        if current.offset + 1 < self.page_size
            && let Some(page_handle) = self.page_slots[current.page_index]
            && let Some(offset) = self.page_pool.pages[page_handle].occupied.next_set_from(current.offset + 1)
        {
            return self.key_from_page_offset(current.page_index, offset);
        }

        let page_index = current.page_index.checked_add(1)?;
        let page_index = self.occupied_pages.next_set_from(page_index)?;
        let page_handle = self.page_slots[page_index]?;
        let offset = self.page_pool.pages[page_handle].occupied.next_set_from(0)?;
        self.key_from_page_offset(page_index, offset)
    }

    /// Update best price after adding visible quantity at `key`.
    fn refresh_best_after_occupy(&mut self, key: LevelKey) {
        // equal or worse prices preserve the existing cache; FIFO priority is
        // represented inside the level rather than in this key.
        self.best = match (self.side, self.best) {
            (_, None) => Some(key),
            (Side::Buy, Some(best)) if key.price > best.price => Some(key),
            (Side::Sell, Some(best)) if key.price < best.price => Some(key),
            (_, best) => best,
        };
    }

    /// Find the current best level by consulting page and level bitmaps.
    fn find_best(&self) -> Option<LevelKey> {
        match self.side {
            Side::Buy => {
                // bids choose the highest occupied page and highest occupied offset.
                let start = self.page_slots.len().checked_sub(1)?;
                let page_index = self.occupied_pages.prev_set_from(start)?;
                let page_handle = self.page_slots[page_index]?;
                let offset = self.page_pool.pages[page_handle].occupied.prev_set_from(self.page_size - 1)?;
                self.key_from_page_offset(page_index, offset)
            }
            Side::Sell => {
                // asks choose the lowest occupied page and lowest occupied offset.
                let page_index = self.occupied_pages.next_set_from(0)?;
                let page_handle = self.page_slots[page_index]?;
                let offset = self.page_pool.pages[page_handle].occupied.next_set_from(0)?;
                self.key_from_page_offset(page_index, offset)
            }
        }
    }

    /// Convert a page and offset back into an absolute price-level key.
    fn key_from_page_offset(&self, page_index: usize, offset: usize) -> Option<LevelKey> {
        let tick = page_index.checked_mul(self.page_size)?.checked_add(offset)? as u64;
        let raw = self.storage_min_price.raw().checked_add(tick.checked_mul(self.tick_size)?)?;
        // the final logical page may be only partially covered by the configured
        // price envelope.
        if raw > self.storage_max_price.raw() {
            return None;
        }
        Some(LevelKey {
            page_index,
            offset,
            price: Price::new_unchecked(raw),
        })
    }
}

/// Visible aggregates and FIFO endpoints for one price level.
///
/// The level does not own order nodes. Its handles point into `OrderArena`,
/// where neighboring orders carry the rest of the FIFO links.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PriceLevel {
    /// Oldest resting order at this price.
    pub(crate) head: Option<OrderHandle>,
    /// Newest resting order at this price.
    pub(crate) tail: Option<OrderHandle>,
    /// Aggregate visible quantity at this price.
    pub(crate) total_qty: u64,
    /// Number of resting orders at this price.
    pub(crate) order_count: u32,
}

impl PriceLevel {
    /// Return whether this level has no visible resting orders.
    pub(crate) const fn is_empty(self) -> bool {
        self.order_count == 0
    }
}

/// Logical address and absolute price of one level inside the paged ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LevelKey {
    /// Absolute page index inside this side's storage envelope.
    pub(crate) page_index: usize,
    /// Price-level offset inside the page.
    pub(crate) offset: usize,
    /// Integer price represented by the page and offset.
    pub(crate) price: Price,
}

/// Packed occupancy bits used to skip empty levels and logical pages.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Bitmap {
    /// Packed 64-bit occupancy words.
    words: Vec<u64>,
}

impl Bitmap {
    /// Create an empty bitmap with enough words for `bits` entries.
    fn new(bits: usize) -> Self {
        Self {
            // round up so a final partial word still has storage for every bit.
            words: vec![0; bits.div_ceil(64)],
        }
    }

    /// Mark one bit as occupied.
    fn set(&mut self, bit: usize) {
        self.words[bit / 64] |= 1_u64 << (bit % 64);
    }

    /// Mark one bit as empty.
    fn clear(&mut self, bit: usize) {
        self.words[bit / 64] &= !(1_u64 << (bit % 64));
    }

    /// Return whether any bit is occupied.
    fn any(&self) -> bool {
        self.words.iter().any(|word| *word != 0)
    }

    /// Return whether `bit` is the only occupied position.
    fn contains_only(&self, bit: usize) -> bool {
        self.words.iter().enumerate().all(|(word_index, word)| {
            let expected = if word_index == bit / 64 { 1_u64 << (bit % 64) } else { 0 };
            *word == expected
        })
    }

    /// Find the lowest set bit at or after `bit`.
    fn next_set_from(&self, bit: usize) -> Option<usize> {
        let mut word_index = bit / 64;
        let mut word = *self.words.get(word_index)?;
        // ignore occupied bits before the requested start point in the first word.
        word &= !0_u64 << (bit % 64);

        loop {
            if word != 0 {
                return Some(word_index * 64 + word.trailing_zeros() as usize);
            }
            word_index += 1;
            word = *self.words.get(word_index)?;
        }
    }

    /// Find the highest set bit at or before `bit`.
    fn prev_set_from(&self, bit: usize) -> Option<usize> {
        let mut word_index = bit / 64;
        let mut word = *self.words.get(word_index)?;
        let keep_bits = bit % 64;
        // ignore occupied bits after the requested start point in the first word.
        word &= u64::MAX >> (63 - keep_bits);

        loop {
            if word != 0 {
                return Some(word_index * 64 + (63 - word.leading_zeros() as usize));
            }
            if word_index == 0 {
                return None;
            }
            word_index -= 1;
            word = self.words[word_index];
        }
    }
}

/// Reusable dense group of adjacent price levels.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Page {
    /// Fixed-size price levels for this page.
    levels: Box<[PriceLevel]>,
    /// Occupancy bitmap for non-empty levels in this page.
    occupied: Bitmap,
}

impl Page {
    /// Allocate one page with `page_size` empty price levels.
    fn new(page_size: usize) -> Self {
        Self {
            levels: vec![PriceLevel::default(); page_size].into_boxed_slice(),
            occupied: Bitmap::new(page_size),
        }
    }

    /// Clear all levels and occupancy before returning a page to the pool.
    fn reset(&mut self) {
        for level in &mut self.levels {
            *level = PriceLevel::default();
        }
        for word in &mut self.occupied.words {
            *word = 0;
        }
    }
}

/// Preallocated physical pages and handles that are currently free.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PagePool {
    /// Preallocated pages available to this side.
    pages: Vec<Page>,
    /// Handles of pages that are currently inactive.
    free: Vec<usize>,
}

impl PagePool {
    /// Preallocate all page storage used by this side.
    fn new(max_active_pages: usize, page_size: usize) -> Self {
        let mut pages = Vec::with_capacity(max_active_pages);
        let mut free = Vec::with_capacity(max_active_pages);
        for index in 0..max_active_pages {
            pages.push(Page::new(page_size));
            // reverse insertion makes pop activate handles in ascending order,
            // keeping page reuse deterministic.
            free.push(max_active_pages - 1 - index);
        }

        Self { pages, free }
    }

    /// Reserve a page for an occupied price range.
    fn activate(&mut self) -> Option<usize> {
        self.free.pop()
    }

    /// Return a page to the reusable pool.
    fn release(&mut self, handle: usize) {
        self.pages[handle].reset();
        self.free.push(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{domain::rd::InstrumentSpec, engine::EngineConfig};

    fn config() -> EngineConfig {
        let instrument = InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument");
        EngineConfig::new(instrument, 100, 1200, 1024, 100, 10, 4).expect("config")
    }

    #[test]
    fn ladder_maps_prices_to_page_and_offset() {
        let side = SideLadder::new(Side::Sell, config()).expect("side");
        let key = side
            .key_for_price(Price::new(1124, &config().instrument()).expect("price"))
            .expect("key");
        assert_eq!(key.page_index, 1);
        assert_eq!(key.offset, 0);
    }

    #[test]
    fn ladder_tracks_best_bid_and_ask_with_bitmaps() {
        let instrument = config().instrument();
        let mut bids = SideLadder::new(Side::Buy, config()).expect("bids");
        let bid_100 = bids.key_for_price(Price::new(100, &instrument).expect("price")).expect("key");
        let bid_1200 = bids.key_for_price(Price::new(1200, &instrument).expect("price")).expect("key");
        bids.get_level_mut_or_activate(bid_100).expect("level").order_count = 1;
        bids.mark_occupied(bid_100).expect("occupied");
        bids.get_level_mut_or_activate(bid_1200).expect("level").order_count = 1;
        bids.mark_occupied(bid_1200).expect("occupied");
        assert_eq!(bids.best.expect("best").price.raw(), 1200);
        bids.mark_empty(bid_1200).expect("empty");
        assert_eq!(bids.best.expect("best").price.raw(), 100);

        let mut asks = SideLadder::new(Side::Sell, config()).expect("asks");
        asks.get_level_mut_or_activate(bid_1200).expect("level").order_count = 1;
        asks.mark_occupied(bid_1200).expect("occupied");
        asks.get_level_mut_or_activate(bid_100).expect("level").order_count = 1;
        asks.mark_occupied(bid_100).expect("occupied");
        assert_eq!(asks.best.expect("best").price.raw(), 100);
    }
}
