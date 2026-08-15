use super::*;
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Builds a generated opposite-side book, sends one aggressive market
    /// IOC order through the real engine, and checks price-time priority.
    /// It also checks maker/taker report pairing, strictly increasing
    /// engine sequence numbers, nonzero resting quantities, total resting
    /// quantity conservation, and per-account aggregate consistency.
    #[test]
    fn property_market_ioc_respects_price_time_priority_and_conserves_book(
        taker_buy in any::<bool>(),
        maker_specs in prop::collection::vec((0_u64..5, 1_u64..25), 1..24),
        taker_qty in 1_u64..500,
    ) {
        let mut engine = TestEngine::new_noop();
        let mut events = Vec::new();
        let taker_side = if taker_buy { Side::Buy } else { Side::Sell };
        let maker_side = if taker_buy { Side::Sell } else { Side::Buy };

        for (index, (price_offset, qty_raw)) in maker_specs.iter().copied().enumerate() {
            execute(
                &mut engine,
                new_order(
                    index as u64 + 1,
                    index as u32 + 1,
                    maker_side,
                    OrderType::Limit,
                    TimeInForce::Gtc,
                    Some(1_000 + price_offset),
                    qty_raw,
                    index as u64 + 1,
                ),
                &mut events,
                "rest maker",
            );
        }

        let mut expected_priority: Vec<_> = maker_specs
            .iter()
            .copied()
            .enumerate()
            .map(|(index, (price_offset, qty_raw))| (index as u64 + 1, 1_000 + price_offset, qty_raw, index))
            .collect();
        expected_priority.sort_by_key(|(_, price_raw, _, index)| (side_order(maker_side, *price_raw), *index));

        execute(
            &mut engine,
            new_order(10_000, 99, taker_side, OrderType::Market, TimeInForce::Ioc, None, taker_qty, 10_000),
            &mut events,
            "take liquidity",
        );

        let mut remaining_taker = taker_qty;
        let mut expected_fills = Vec::new();
        for (maker_id, price_raw, maker_qty, _) in expected_priority {
            if remaining_taker == 0 {
                break;
            }
            let fill_qty = remaining_taker.min(maker_qty);
            expected_fills.push((maker_id, price_raw, fill_qty));
            remaining_taker -= fill_qty;
        }

        let maker_reports: Vec<_> = events
            .iter()
            .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() != 10_000)
            .map(|report| (report.client_order_id.raw(), report.fill_price.expect("maker fill price").raw(), report.fill_qty))
            .collect();
        let taker_reports: Vec<_> = events
            .iter()
            .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() == 10_000)
            .map(|report| (report.fill_price.expect("taker fill price").raw(), report.fill_qty))
            .collect();

        prop_assert_eq!(maker_reports, expected_fills.clone());
        prop_assert_eq!(
            taker_reports,
            expected_fills
                .iter()
                .map(|(_, price_raw, fill_qty)| (*price_raw, *fill_qty))
                .collect::<Vec<_>>()
        );
        prop_assert!(events.windows(2).all(|pair| pair[0].engine_seq.raw() < pair[1].engine_seq.raw()));

        let total_resting: u64 = maker_specs.iter().map(|(_, qty_raw)| *qty_raw).sum();
        let total_filled = taker_qty.min(total_resting);
        let rows = resting_rows(&engine);
        prop_assert!(rows.iter().all(|(_, _, _, _, qty_raw)| *qty_raw > 0));
        prop_assert_eq!(rows.iter().map(|(_, _, _, _, qty_raw)| *qty_raw).sum::<u64>(), total_resting - total_filled);
        prop_assert_eq!(engine.resting_order_count(), rows.len());
        assert_account_aggregates_match_rows(&engine);
    }

    /// Uses file-backed market-data output so the property checks the
    /// actual encoded SBE feed, not only execution reports. It builds a
    /// generated one-sided book, executes one aggressive market IOC, then
    /// asserts that TradePrints carry the expected maker, taker, price, and
    /// quantity, every L2 mutation is immediately followed by an L1
    /// snapshot, L1 ends at the final top of book, and market-data sequence
    /// numbers plus prev links are monotonic.
    #[test]
    fn property_market_data_feed_matches_generated_matching_semantics(
        taker_buy in any::<bool>(),
        maker_specs in prop::collection::vec((0_u64..5, 1_u64..25), 1..24),
        taker_qty in 1_u64..500,
    ) {
        let path = temp_path("property-market-data");
        let mut engine = TestEngine::new_file_market_data(path.clone());
        let mut events = Vec::new();
        let taker_side = if taker_buy { Side::Buy } else { Side::Sell };
        let maker_side = if taker_buy { Side::Sell } else { Side::Buy };
        let mut expected_l2 = Vec::new();
        let mut level_qty: HashMap<u64, u64> = HashMap::new();

        for (index, (price_offset, qty_raw)) in maker_specs.iter().copied().enumerate() {
            let price_raw = 1_000 + price_offset;
            execute(
                &mut engine,
                new_order(
                    index as u64 + 1,
                    index as u32 + 1,
                    maker_side,
                    OrderType::Limit,
                    TimeInForce::Gtc,
                    Some(price_raw),
                    qty_raw,
                    index as u64 + 1,
                ),
                &mut events,
                "rest maker",
            );
            let qty = level_qty.entry(price_raw).or_insert(0);
            *qty += qty_raw;
            expected_l2.push((maker_side, price_raw, *qty));
        }

        let mut expected_priority: Vec<_> = maker_specs
            .iter()
            .copied()
            .enumerate()
            .map(|(index, (price_offset, qty_raw))| (index as u64 + 1, 1_000 + price_offset, qty_raw, index))
            .collect();
        expected_priority.sort_by_key(|(_, price_raw, _, index)| (side_order(maker_side, *price_raw), *index));

        execute(
            &mut engine,
            new_order(10_000, 99, taker_side, OrderType::Market, TimeInForce::Ioc, None, taker_qty, 10_000),
            &mut events,
            "take liquidity",
        );

        let mut remaining_taker = taker_qty;
        let mut expected_trades = Vec::new();
        for (maker_id, price_raw, maker_qty, _) in expected_priority {
            if remaining_taker == 0 {
                break;
            }
            let fill_qty = remaining_taker.min(maker_qty);
            expected_trades.push((maker_id, 10_000, price_raw, fill_qty));
            remaining_taker -= fill_qty;
            let qty = level_qty.get_mut(&price_raw).expect("level qty");
            *qty -= fill_qty;
            expected_l2.push((maker_side, price_raw, *qty));
        }

        engine.flush_market_data();
        let captured = read_market_data(&path);
        let _ = std::fs::remove_file(path);

        prop_assert_eq!(
            captured.iter().map(|event| event.md_seq()).collect::<Vec<_>>(),
            (1..=captured.len() as u64).collect::<Vec<_>>()
        );
        prop_assert!(captured.windows(2).all(|pair| pair[0].engine_seq() < pair[1].engine_seq()));
        for event in &captured {
            match *event {
                CapturedMarketData::L1 { md_seq, prev_md_seq, .. } | CapturedMarketData::L2 { md_seq, prev_md_seq, .. } => {
                    prop_assert_eq!(prev_md_seq, md_seq - 1);
                }
                CapturedMarketData::Trade { .. } => {}
            }
        }

        let l2_count = captured
            .iter()
            .filter(|event| matches!(event, CapturedMarketData::L2 { .. }))
            .count();
        let l1_count = captured
            .iter()
            .filter(|event| matches!(event, CapturedMarketData::L1 { .. }))
            .count();
        prop_assert_eq!(l1_count, l2_count);
        for pair in captured.windows(2) {
            if matches!(pair[0], CapturedMarketData::L2 { .. }) {
                let l1_follows = matches!(pair[1], CapturedMarketData::L1 { .. });
                prop_assert!(l1_follows);
            }
        }

        let trades = captured
            .iter()
            .filter_map(|event| match *event {
                CapturedMarketData::Trade {
                    trade_id,
                    maker_order_id,
                    taker_order_id,
                    price,
                    qty,
                    ..
                } => Some((trade_id, maker_order_id, taker_order_id, price, qty)),
                CapturedMarketData::L1 { .. } | CapturedMarketData::L2 { .. } => None,
            })
            .collect::<Vec<_>>();
        prop_assert_eq!(
            trades,
            expected_trades
                .iter()
                .enumerate()
                .map(|(index, (maker_order_id, taker_order_id, price, qty))| {
                    (index as u64 + 1, *maker_order_id, *taker_order_id, *price, *qty)
                })
                .collect::<Vec<_>>()
        );

        let l2_updates = captured
            .iter()
            .filter_map(|event| match *event {
                CapturedMarketData::L2 { side, price, qty, .. } => Some((side, price, qty)),
                CapturedMarketData::Trade { .. } | CapturedMarketData::L1 { .. } => None,
            })
            .collect::<Vec<_>>();
        prop_assert_eq!(l2_updates, expected_l2);

        let last_l1 = captured.iter().rev().find_map(|event| match *event {
            CapturedMarketData::L1 {
                bid_price,
                bid_qty,
                ask_price,
                ask_qty,
                ..
            } => Some((bid_price, bid_qty, ask_price, ask_qty)),
            CapturedMarketData::Trade { .. } | CapturedMarketData::L2 { .. } => None,
        });
        if let Some((bid_price, bid_qty, ask_price, ask_qty)) = last_l1 {
            let final_bid = engine.best_bid().map_or((0, 0), |(price, qty)| (price.raw(), qty));
            let final_ask = engine.best_ask().map_or((0, 0), |(price, qty)| (price.raw(), qty));
            prop_assert_eq!((bid_price, bid_qty), final_bid);
            prop_assert_eq!((ask_price, ask_qty), final_ask);
        }
    }

    /// Builds a generated one-sided book across several accounts, runs a
    /// mass cancel for one account, and checks that only that account's
    /// resting orders disappear. The remaining rows must keep deterministic
    /// book order and the account open-order/notional aggregates must still
    /// match the visible book.
    #[test]
    fn property_mass_cancel_removes_only_target_account(
        target_account in 1_u32..8,
        order_specs in prop::collection::vec((1_u32..8, 0_u64..5, 1_u64..25), 1..32),
    ) {
        let mut engine = TestEngine::new_noop();
        let mut events = Vec::new();
        let mut expected = Vec::new();

        for (index, (account, price_offset, qty_raw)) in order_specs.iter().copied().enumerate() {
            let side = Side::Buy;
            let id = index as u64 + 1;
            execute(
                &mut engine,
                new_order(id, account, side, OrderType::Limit, TimeInForce::Gtc, Some(1_000 + price_offset), qty_raw, id),
                &mut events,
                "rest",
            );
            expected.push((id, account, side, 1_000 + price_offset, qty_raw));
        }

        execute(&mut engine, mass_cancel(10_000, target_account), &mut events, "mass cancel");
        expected.retain(|(_, account, _, _, _)| *account != target_account);
        expected.sort_by_key(|(id, _, side, price_raw, _)| (*side == Side::Sell, side_order(*side, *price_raw), *id));

        prop_assert_eq!(resting_rows(&engine), expected);
        prop_assert!(resting_rows(&engine).iter().all(|(_, account, _, _, _)| *account != target_account));
        assert_account_aggregates_match_rows(&engine);
    }

    /// Runs a generated mixed stream of adds, cancels, replaces, mass
    /// cancels, duplicate IDs, unknown order references, market orders, IOC
    /// orders, GTC residuals, post-only orders, and storage-envelope
    /// rejects through one persistent engine. After every command it checks
    /// report sequencing, maker/taker trade pairing, no crossed book,
    /// unique and positive resting rows, deterministic book ordering, best
    /// quote aggregates, and per-account open count/notional consistency.
    #[test]
    fn property_mixed_command_stream_preserves_core_book_invariants(
        commands in prop::collection::vec(generated_command_strategy(), 1..96),
    ) {
        let mut engine = TestEngine::new_noop();
        let mut events = Vec::new();
        let mut last_engine_seq = 0;

        for (index, command) in commands.into_iter().enumerate() {
            execute(
                &mut engine,
                command.into_entry(index as u64 + 1),
                &mut events,
                "generated mixed command",
            );
            assert_reports_are_strictly_sequenced(&events, &mut last_engine_seq);
            assert_trade_reports_are_paired(&events);
            assert_core_book_invariants(&engine);
        }
    }

    /// Checks the documented cancel-replace priority rules with generated
    /// quantities. Same-price quantity reductions must keep the order's
    /// FIFO position, while price changes and quantity increases must lose
    /// priority against orders that were already resting at the destination
    /// price level.
    #[test]
    fn property_cancel_replace_priority_rules_match_documented_semantics(
        first_qty in 2_u64..40,
        reduced_qty in 1_u64..20,
        competing_qty in 1_u64..40,
        increase_delta in 1_u64..20,
    ) {
        let reduced_qty = reduced_qty.min(first_qty);

        let mut reduction_engine = TestEngine::new_noop();
        let mut reduction_events = Vec::new();
        execute(&mut reduction_engine, new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), first_qty, 1), &mut reduction_events, "rest first");
        execute(&mut reduction_engine, new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), competing_qty, 2), &mut reduction_events, "rest second");
        execute(&mut reduction_engine, replace(11, 1, 1, 1_000, reduced_qty), &mut reduction_events, "reduce");
        execute(&mut reduction_engine, new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, reduced_qty, 3), &mut reduction_events, "take reduced");
        prop_assert_eq!(trade_report_makers(&reduction_events, 3), vec![11]);
        assert_account_aggregates_match_rows(&reduction_engine);

        let mut price_change_engine = TestEngine::new_noop();
        let mut price_change_events = Vec::new();
        execute(&mut price_change_engine, new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_001), competing_qty, 1), &mut price_change_events, "rest competing destination");
        execute(&mut price_change_engine, new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), first_qty, 2), &mut price_change_events, "rest original");
        execute(&mut price_change_engine, replace(22, 2, 2, 1_001, first_qty), &mut price_change_events, "price change");
        execute(&mut price_change_engine, new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 1, 3), &mut price_change_events, "take changed");
        prop_assert_eq!(trade_report_makers(&price_change_events, 3), vec![1]);
        assert_account_aggregates_match_rows(&price_change_engine);

        let mut increase_engine = TestEngine::new_noop();
        let mut increase_events = Vec::new();
        execute(&mut increase_engine, new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), first_qty, 1), &mut increase_events, "rest original");
        execute(&mut increase_engine, new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), competing_qty, 2), &mut increase_events, "rest competing");
        execute(&mut increase_engine, replace(11, 1, 1, 1_000, first_qty + increase_delta), &mut increase_events, "increase");
        execute(&mut increase_engine, new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 1, 3), &mut increase_events, "take increased");
        prop_assert_eq!(trade_report_makers(&increase_events, 3), vec![2]);
        assert_account_aggregates_match_rows(&increase_engine);
    }
}
