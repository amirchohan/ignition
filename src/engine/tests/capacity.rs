use super::*;

#[test]
fn full_order_arena_rejects_without_mutation_and_reuses_slot_after_cancel() {
    let mut engine = TestEngine::new_noop_with_config(capacity_config(1, 1024, 2));
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "fill arena",
    );
    let before = resting_rows(&engine);

    let outcome = engine.execute_outcome(
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1001), 5, 2),
        &mut events,
        "capacity reject",
    );

    assert_eq!(outcome, EngineExecution::CapacityExceeded(CapacityResource::RestingOrders));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].reject_reason, RejectReason::KillSwitchActive);
    assert_eq!(resting_rows(&engine), before);
    assert_core_book_invariants(&engine);

    execute(&mut engine, cancel(3, 1, 1), &mut events, "cancel after pressure");
    execute(
        &mut engine,
        new_order(4, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1001), 5, 4),
        &mut events,
        "reuse slot",
    );
    assert_eq!(resting_rows(&engine)[0].0, 4);
}

#[test]
fn full_page_pool_rejects_without_leaking_the_reserved_order_slot() {
    let mut engine = TestEngine::new_noop_with_config(capacity_config(2, 2, 1));
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "activate page",
    );
    let before = resting_rows(&engine);

    let outcome = engine.execute_outcome(
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1002), 5, 2),
        &mut events,
        "page reject",
    );

    assert_eq!(outcome, EngineExecution::CapacityExceeded(CapacityResource::ActivePages));
    assert_eq!(events[0].reject_reason, RejectReason::KillSwitchActive);
    assert_eq!(resting_rows(&engine), before);
    assert_eq!(engine.account_open_order_count(2), Some(0));

    execute(&mut engine, cancel(3, 1, 1), &mut events, "release page");
    execute(
        &mut engine,
        new_order(4, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1002), 5, 4),
        &mut events,
        "slot remains reusable",
    );
    assert_eq!(resting_rows(&engine)[0].0, 4);
}

#[test]
fn full_arena_allows_crossing_gtc_to_reuse_a_filled_maker_slot() {
    let mut engine = TestEngine::new_noop_with_config(capacity_config(1, 1024, 1));
    let mut events = Vec::new();
    execute(
        &mut engine,
        new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 5, 1),
        &mut events,
        "rest maker",
    );

    let outcome = engine.execute_outcome(
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 2),
        &mut events,
        "cross and rest",
    );

    assert_eq!(outcome, EngineExecution::Processed);
    assert_eq!(resting_rows(&engine), vec![(2, 2, Side::Buy, 1000, 5)]);
    assert!(events.iter().all(|report| report.reject_reason == RejectReason::None));
}

#[test]
fn exhausted_storage_does_not_block_commands_that_need_no_new_resource() {
    let mut arena_full = TestEngine::new_noop_with_config(capacity_config(1, 2, 1));
    let mut events = Vec::new();
    execute(
        &mut arena_full,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "fill storage",
    );

    let outcome = arena_full.execute_outcome(replace(11, 1, 1, 1000, 8), &mut events, "preserve replace");
    assert_eq!(outcome, EngineExecution::Processed);
    let outcome = arena_full.execute_outcome(
        new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Ioc, Some(1000), 3, 2),
        &mut events,
        "ioc needs no storage",
    );
    assert_eq!(outcome, EngineExecution::Processed);
    assert_eq!(resting_rows(&arena_full), vec![(11, 1, Side::Buy, 1000, 5)]);

    let mut page_full = TestEngine::new_noop_with_config(capacity_config(2, 2, 1));
    execute(
        &mut page_full,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "activate page",
    );
    let outcome = page_full.execute_outcome(
        new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1001), 5, 2),
        &mut events,
        "reuse active page",
    );
    assert_eq!(outcome, EngineExecution::Processed);
    assert_eq!(page_full.resting_order_count(), 2);
}

#[test]
fn replacement_capacity_preflight_accounts_for_source_page_release() {
    let mut blocked = TestEngine::new_noop_with_config(capacity_config(3, 2, 1));
    let mut events = Vec::new();
    for (id, price_raw) in [(1, 1000), (2, 1001)] {
        execute(
            &mut blocked,
            new_order(
                id,
                id as u32,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(price_raw),
                10,
                id,
            ),
            &mut events,
            "seed shared page",
        );
    }
    let before = resting_rows(&blocked);
    let outcome = blocked.execute_outcome(replace(11, 1, 1, 1002, 10), &mut events, "blocked replace");
    assert_eq!(outcome, EngineExecution::CapacityExceeded(CapacityResource::ActivePages));
    assert_eq!(events[0].reject_reason, RejectReason::KillSwitchActive);
    assert_eq!(resting_rows(&blocked), before);

    let mut reusable = TestEngine::new_noop_with_config(capacity_config(2, 2, 1));
    execute(
        &mut reusable,
        new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
        &mut events,
        "seed sole page",
    );
    let outcome = reusable.execute_outcome(replace(11, 1, 1, 1002, 10), &mut events, "reuse source page");
    assert_eq!(outcome, EngineExecution::Processed);
    assert_eq!(resting_rows(&reusable), vec![(11, 1, Side::Buy, 1002, 10)]);
}
