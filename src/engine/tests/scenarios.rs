use super::*;

#[test]
fn account_scoped_order_ids_coexist_and_cancel_independently() {
    let mut engine = TestEngine::new_noop();
    let mut events = Vec::new();
    for (account, price_raw) in [(1, 1_000), (2, 999)] {
        execute(
            &mut engine,
            new_order(
                7,
                account,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(price_raw),
                10,
                u64::from(account),
            ),
            &mut events,
            "rest account-scoped order",
        );
        assert_eq!(events[0].reject_reason, RejectReason::None);
    }

    assert_eq!(engine.resting_order_count(), 2);
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(1).expect("account"), OrderId::new(7).expect("order"))
            .is_some()
    );
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(2).expect("account"), OrderId::new(7).expect("order"))
            .is_some()
    );

    execute(&mut engine, cancel(8, 1, 7), &mut events, "cancel first account");

    assert_eq!(engine.resting_order_count(), 1);
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(1).expect("account"), OrderId::new(7).expect("order"))
            .is_none()
    );
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(2).expect("account"), OrderId::new(7).expect("order"))
            .is_some()
    );
    assert_eq!(resting_rows(&engine), vec![(7, 2, Side::Buy, 999, 10)]);

    execute(
        &mut engine,
        new_order(7, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(998), 5, 3),
        &mut events,
        "reuse terminal account-scoped ID",
    );
    execute(&mut engine, mass_cancel(9, 1), &mut events, "mass cancel first account");

    assert_eq!(resting_rows(&engine), vec![(7, 2, Side::Buy, 999, 10)]);
}

#[test]
fn duplicate_order_id_is_rejected_only_within_same_account() {
    let mut engine = TestEngine::new_noop();
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(5, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10, 1),
        &mut events,
        "rest original",
    );

    execute(
        &mut engine,
        new_order(5, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(999), 10, 2),
        &mut events,
        "reject same-account duplicate",
    );
    assert_eq!(events[0].reject_reason, RejectReason::Malformed);

    execute(
        &mut engine,
        new_order(5, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(999), 10, 3),
        &mut events,
        "accept cross-account duplicate",
    );
    assert_eq!(events[0].reject_reason, RejectReason::None);
    assert_eq!(engine.resting_order_count(), 2);
}

#[test]
fn replace_and_trade_resolve_account_scoped_order_identity() {
    let mut engine = TestEngine::new_noop();
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(7, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10, 1),
        &mut events,
        "rest maker",
    );
    execute(
        &mut engine,
        new_order(8, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(999), 10, 2),
        &mut events,
        "rest replacement-id collision",
    );

    execute(&mut engine, replace(8, 1, 7, 1_000, 8), &mut events, "replace within account");
    assert_eq!(events[0].exec_type, ExecType::Replaced);
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(1).expect("account"), OrderId::new(8).expect("order"))
            .is_some()
    );
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(2).expect("account"), OrderId::new(8).expect("order"))
            .is_some()
    );

    execute(
        &mut engine,
        new_order(8, 3, Side::Buy, OrderType::Limit, TimeInForce::Ioc, Some(1_000), 8, 3),
        &mut events,
        "trade with same numeric maker and taker ID",
    );
    let trade_reports = events
        .iter()
        .filter(|report| report.exec_type == ExecType::Trade)
        .collect::<Vec<_>>();
    assert_eq!(trade_reports.len(), 2);
    assert!(trade_reports.iter().all(|report| report.client_order_id.raw() == 8));
    assert_eq!(
        trade_reports.iter().map(|report| report.account_id.raw()).collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(1).expect("account"), OrderId::new(8).expect("order"))
            .is_none()
    );
    assert!(
        engine
            .engine
            .resting_order(AccountId::new(2).expect("account"), OrderId::new(8).expect("order"))
            .is_some()
    );
}

#[test]
fn passive_orders_rest_and_update_top_of_book() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "execute",
    );

    assert_eq!(engine.best_bid(), Some((price(1000), 10)));
    assert_eq!(engine.best_ask(), None);
    assert_eq!(engine.resting_order_count(), 1);
    assert_eq!(engine.account_open_order_count(1), Some(1));
    assert_eq!(engine.account_open_notional(1), Some(10_000));
}

#[test]
fn non_best_level_mutation_reemits_unchanged_top_of_book() {
    let path = temp_path("unchanged-top-of-book");
    let mut engine = TestEngine::new_file_market_data(path.clone());
    let mut events = Vec::new();

    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest best bid",
    );
    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(999), 5, 2),
        &mut events,
        "rest non-best bid",
    );

    engine.flush_market_data();
    let captured = read_market_data(&path);
    let _ = std::fs::remove_file(path);

    assert_eq!(captured.len(), 4);
    assert!(matches!(captured[0], CapturedMarketData::L2 { price: 1000, qty: 10, .. }));
    assert!(matches!(captured[2], CapturedMarketData::L2 { price: 999, qty: 5, .. }));
    for event in [captured[1], captured[3]] {
        assert!(matches!(
            event,
            CapturedMarketData::L1 {
                bid_price: 1000,
                bid_qty: 10,
                ask_price: 0,
                ask_qty: 0,
                ..
            }
        ));
    }
    assert_eq!(captured.iter().map(|event| event.md_seq()).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
}

#[test]
fn snapshot_streams_rows_in_price_time_order() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    for (id, account, side, price_raw, qty_raw) in [
        (1, 1, Side::Buy, 1000, 10),
        (2, 2, Side::Buy, 1001, 5),
        (3, 3, Side::Buy, 1001, 7),
        (4, 4, Side::Sell, 1100, 9),
    ] {
        execute(
            &mut engine,
            new_order(id, account, side, OrderType::Limit, TimeInForce::Gtc, Some(price_raw), qty_raw, id),
            &mut events,
            "rest",
        );
    }

    let mut rows = Vec::new();
    engine
        .engine
        .try_for_each_resting_order(|row| {
            rows.push((row.order_id.raw(), row.side, row.price.raw(), row.remaining_qty));
            Ok::<(), ()>(())
        })
        .expect("snapshot rows");

    assert_eq!(
        rows,
        vec![
            (2, Side::Buy, 1001, 5),
            (3, Side::Buy, 1001, 7),
            (1, Side::Buy, 1000, 10),
            (4, Side::Sell, 1100, 9),
        ]
    );
}

#[test]
fn execute_clears_caller_owned_event_buffer() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = vec![crate::domain::trading::order::ExecutionReport {
        engine_seq: EngineSeq::new(99),
        client_order_id: OrderId::new(99).expect("order"),
        account_id: AccountId::new(99).expect("account"),
        exec_type: ExecType::Rejected,
        ord_status: OrdStatus::Rejected,
        reject_reason: RejectReason::Malformed,
        fill_price: None,
        fill_qty: 0,
        remaining_qty: 0,
    }];

    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 1),
        &mut events,
        "execute",
    );

    let reports = exec_reports(&events);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].client_order_id.raw(), 1);
}

#[test]
fn matching_preserves_fifo_at_price_level() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    for id in [1, 2] {
        execute(
            &mut engine,
            new_order(id, id as u32, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, id),
            &mut events,
            "rest",
        );
    }

    execute(
        &mut engine,
        new_order(3, 3, Side::Buy, OrderType::Limit, TimeInForce::Ioc, Some(1000), 15, 3),
        &mut events,
        "match",
    );

    assert_eq!(trade_report_makers(&events, 3), vec![1, 2]);
    let taker_reports = reports_for_order(&events, 3);
    assert_eq!(taker_reports.len(), 2);
    assert_eq!(taker_reports[0].exec_type, ExecType::Trade);
    assert_eq!(taker_reports[0].ord_status, OrdStatus::PartiallyFilled);
    assert_eq!(taker_reports[0].fill_price, Some(price(1000)));
    assert_eq!(taker_reports[0].fill_qty, 10);
    assert_eq!(taker_reports[0].remaining_qty, 5);
    assert_eq!(taker_reports[1].exec_type, ExecType::Trade);
    assert_eq!(taker_reports[1].ord_status, OrdStatus::Filled);
    assert_eq!(taker_reports[1].fill_price, Some(price(1000)));
    assert_eq!(taker_reports[1].fill_qty, 5);
    assert_eq!(taker_reports[1].remaining_qty, 0);
    assert_eq!(engine.best_ask(), Some((price(1000), 5)));
}

#[test]
fn fill_emits_trade_l2_delete_and_l1_after_unlink() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );

    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 2),
        &mut events,
        "match",
    );

    assert_eq!(engine.account_open_order_count(1), Some(0));
    assert_eq!(engine.account_open_notional(1), Some(0));
    assert_eq!(engine.best_ask(), None);
}

#[test]
fn cancel_unlinks_head_middle_and_tail_without_scanning() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    for id in [1, 2, 3] {
        execute(
            &mut engine,
            new_order(id, id as u32, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, id),
            &mut events,
            "rest",
        );
    }

    execute(&mut engine, cancel(20, 2, 2), &mut events, "cancel middle");
    execute(&mut engine, cancel(21, 1, 1), &mut events, "cancel head");
    execute(
        &mut engine,
        new_order(4, 4, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 4),
        &mut events,
        "match tail",
    );

    assert_eq!(trade_report_makers(&events, 4), vec![3]);
    assert_eq!(engine.resting_order_count(), 0);
}

#[test]
fn partial_fill_reduces_account_open_notional() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );
    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Ioc, Some(1000), 4, 2),
        &mut events,
        "partial fill",
    );

    assert_eq!(engine.account_open_order_count(1), Some(1));
    assert_eq!(engine.account_open_notional(1), Some(6_000));
}

#[test]
fn mass_cancel_only_removes_target_account_orders() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 7, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );
    execute(
        &mut engine,
        new_order(2, 8, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1001), 10, 2),
        &mut events,
        "rest",
    );

    execute(&mut engine, mass_cancel(50, 7), &mut events, "mass cancel");

    assert_eq!(engine.resting_order_count(), 1);
    assert_eq!(engine.best_bid(), Some((price(1001), 10)));
}

#[test]
fn same_price_qty_reduction_replace_preserves_priority() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );
    execute(
        &mut engine,
        new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 2),
        &mut events,
        "rest",
    );
    execute(&mut engine, replace(11, 1, 1, 1000, 5), &mut events, "replace");

    execute(
        &mut engine,
        new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 5, 3),
        &mut events,
        "match",
    );

    assert_eq!(trade_report_makers(&events, 3), vec![11]);
    assert_eq!(engine.best_ask(), Some((price(1000), 10)));
    assert_eq!(engine.account_open_order_count(1), Some(0));
    assert_eq!(engine.account_open_notional(1), Some(0));
}

#[test]
fn same_price_qty_reduction_publishes_aggregate_l2_qty() {
    let path = temp_path("replace-aggregate-l2");
    let mut engine = TestEngine::new_file_market_data(path.clone());
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest first",
    );
    execute(
        &mut engine,
        new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 2),
        &mut events,
        "rest second",
    );
    execute(&mut engine, replace(11, 1, 1, 1000, 5), &mut events, "reduce first");

    engine.flush_market_data();
    let captured = read_market_data(&path);
    let _ = std::fs::remove_file(path);
    let level_quantities = captured
        .iter()
        .filter_map(|event| match event {
            CapturedMarketData::L2 {
                side: Side::Sell,
                price: 1000,
                qty,
                ..
            } => Some(*qty),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(level_quantities, vec![10, 20, 15]);
    assert_eq!(engine.best_ask(), Some((price(1000), 15)));
}

#[test]
fn same_price_same_qty_replace_does_not_emit_market_data() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );

    execute(&mut engine, replace(11, 1, 1, 1000, 10), &mut events, "replace");

    assert_eq!(exec_reports(&events).len(), 1);
}

#[test]
fn market_data_and_execution_reports_share_engine_sequence() {
    let path = temp_path("global-outbound-sequence");
    let mut engine = TestEngine::new_file_market_data(path.clone());
    let mut events = Vec::new();

    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest maker",
    );
    assert_eq!(
        exec_reports(&events).iter().map(|event| event.engine_seq.raw()).collect::<Vec<_>>(),
        vec![3]
    );

    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 4, 2),
        &mut events,
        "take liquidity",
    );
    assert_eq!(
        exec_reports(&events).iter().map(|event| event.engine_seq.raw()).collect::<Vec<_>>(),
        vec![7, 8]
    );

    engine.flush_market_data();
    let captured = read_market_data(&path);
    let _ = std::fs::remove_file(path);

    assert_eq!(
        captured.iter().map(|event| event.engine_seq()).collect::<Vec<_>>(),
        vec![1, 2, 4, 5, 6]
    );
}

#[test]
fn price_change_replace_loses_priority() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );
    execute(
        &mut engine,
        new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 2),
        &mut events,
        "rest",
    );
    execute(&mut engine, replace(11, 1, 1, 1001, 10), &mut events, "replace");

    execute(
        &mut engine,
        new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 3),
        &mut events,
        "match",
    );

    assert_eq!(trade_report_makers(&events, 3), vec![2]);
}

#[test]
fn storage_envelope_rejects_unrepresentable_prices() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(10_001), 10, 1),
        &mut events,
        "execute",
    );
    let reports = exec_reports(&events);

    assert_eq!(reports[0].exec_type, ExecType::Rejected);
    assert_eq!(reports[0].ord_status, OrdStatus::Rejected);
    assert_eq!(reports[0].reject_reason, RejectReason::InvalidPrice);
    assert_eq!(engine.resting_order_count(), 0);
}

#[test]
fn post_only_crossing_order_cancels_without_resting() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );
    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::PostOnly, Some(1000), 10, 2),
        &mut events,
        "post only",
    );
    let reports = exec_reports(&events);

    assert_eq!(reports[0].exec_type, ExecType::Canceled);
    assert_eq!(reports[0].ord_status, OrdStatus::Canceled);
    assert_eq!(engine.best_ask(), Some((price(1000), 10)));
}

#[test]
fn market_no_fill_expires_without_fill_price() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 1),
        &mut events,
        "execute",
    );
    let reports = reports_for_order(&events, 1);

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].exec_type, ExecType::Expired);
    assert_eq!(reports[0].ord_status, OrdStatus::Expired);
    assert_eq!(reports[0].fill_price, None);
    assert_eq!(reports[0].fill_qty, 0);
    assert_eq!(reports[0].remaining_qty, 0);
}

#[test]
fn market_partial_fill_expires_residual_on_final_trade_report() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );

    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 15, 2),
        &mut events,
        "execute",
    );
    let reports = reports_for_order(&events, 2);

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].exec_type, ExecType::Trade);
    assert_eq!(reports[0].ord_status, OrdStatus::Expired);
    assert_eq!(reports[0].fill_price, Some(price(1000)));
    assert_eq!(reports[0].fill_qty, 10);
    assert_eq!(reports[0].remaining_qty, 0);
}

#[test]
fn gtc_partial_fill_keeps_live_remaining_qty_on_trade_report() {
    let Some(mut engine) = TestEngine::new() else {
        return;
    };
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "rest",
    );

    execute(
        &mut engine,
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 15, 2),
        &mut events,
        "execute",
    );
    let reports = reports_for_order(&events, 2);

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].exec_type, ExecType::Trade);
    assert_eq!(reports[0].ord_status, OrdStatus::PartiallyFilled);
    assert_eq!(reports[0].fill_price, Some(price(1000)));
    assert_eq!(reports[0].fill_qty, 10);
    assert_eq!(reports[0].remaining_qty, 5);
    assert_eq!(engine.best_bid(), Some((price(1000), 5)));
}
