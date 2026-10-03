"""分级配额：层级份额、向上借、到期归还、降级、回滚与恢复。"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import replace

import pytest

from psdline.app import build_platform
from psdline.clock import ManualClock
from psdline.config import config_from_mapping, config_to_mapping, default_config
from psdline.errors import ConfigError, RailError
from psdline.quota import QuotaEngine, QuotaLedger
from psdline.quota.scopes import line_scope, station_scope, train_scope
from psdline.store import JsonStore

LIMIT_KEYS = ("arrival", "depart")


def make_engine(root, clock, *, line=6, station=2, train=2, period_s=10.0, repay_mode="auto", grace_periods=1):
    ledger = QuotaLedger(JsonStore(root), clock, segment_limit=8)
    return QuotaEngine(
        ledger,
        clock,
        line_id="L1",
        period_s=period_s,
        repay_mode=repay_mode,
        grace_periods=grace_periods,
        limits={
            ("line", kind): line for kind in LIMIT_KEYS
        }
        | {("station", kind): station for kind in LIMIT_KEYS}
        | {("train", kind): train for kind in LIMIT_KEYS},
    )


def make_platform(tmp_path, clock, **quota_overrides):
    config = default_config()
    if quota_overrides:
        config = replace(config, quota=replace(config.quota, **quota_overrides))
    return build_platform(config, clock=clock, storage_root=str(tmp_path / "state"))


# ---- 层级消费与借用 -----------------------------------------------------


def test_consume_decrements_all_levels(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock)
    decision = engine.acquire("T01", "ST1", "arrival")
    assert decision["ok"] and decision["loans"] == []
    assert engine.remaining(train_scope("T01"), "arrival") == 1
    assert engine.remaining(station_scope("ST1"), "arrival") == 1
    assert engine.remaining(line_scope("L1"), "arrival") == 5


def test_station_borrows_from_line_when_short(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, station=1, train=5)
    assert engine.acquire("T01", "ST1", "arrival")["ok"]
    decision = engine.acquire("T02", "ST1", "arrival")
    assert decision["ok"] and len(decision["loans"]) == 1
    loan = engine.loans()[0]
    assert loan["borrower"] == station_scope("ST1")
    assert loan["lender"] == line_scope("L1")
    assert loan["status"] == "open" and loan["mode"] == "auto"
    assert engine.remaining(station_scope("ST1"), "arrival") == 0
    assert engine.remaining(line_scope("L1"), "arrival") == 4


def test_train_borrows_from_station_before_station_borrows(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, station=1, train=1)
    assert engine.acquire("T01", "ST1", "arrival")["ok"]
    # 列车与站台同时见底：先补站台（站台←线路），再补列车（列车←站台）。
    decision = engine.acquire("T01", "ST1", "arrival")
    assert decision["ok"] and len(decision["loans"]) == 2
    loans = {loan["id"]: loan for loan in engine.loans()}
    first, second = sorted(loans)
    assert loans[first]["borrower"] == station_scope("ST1")
    assert loans[second]["borrower"] == train_scope("T01")
    assert loans[second]["lender"] == station_scope("ST1")


def test_line_exhausted_denies_and_records(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, line=2, station=5, train=5)
    assert engine.acquire("T01", "ST1", "arrival")["ok"]
    assert engine.acquire("T02", "ST1", "arrival")["ok"]
    decision = engine.acquire("T03", "ST1", "arrival")
    assert not decision["ok"] and "线路配额已用完" in decision["reason"]
    denies = [r for r in engine.ledger_tail(10) if r["op"] == "deny"]
    assert denies and denies[-1]["train_id"] == "T03"


# ---- 到期归还 -----------------------------------------------------------


def test_auto_loan_repaid_at_rollover(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, station=1, train=5)
    engine.acquire("T01", "ST1", "arrival")
    engine.acquire("T02", "ST1", "arrival")
    assert len(engine.loans()) == 1
    clock.advance(10)
    tick = engine.tick()
    assert tick["rolled"] and tick["loans_open"] == 0
    assert engine.loans() == []
    # 还款冻结了站台新周期份额：本期不可用，下个周期恢复。
    assert engine.remaining(station_scope("ST1"), "arrival") == 0
    clock.advance(10)
    engine.tick()
    assert engine.remaining(station_scope("ST1"), "arrival") == 1


def test_manual_loan_waits_for_confirmation(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, station=1, train=5, repay_mode="manual")
    engine.acquire("T01", "ST1", "arrival")
    engine.acquire("T02", "ST1", "arrival")
    clock.advance(10)
    engine.tick()
    loan = engine.loans()[0]
    assert loan["status"] == "due"
    outcome = engine.repay(loan["id"])
    assert outcome["repaid"] == 1 and outcome["degrade"] is None
    assert engine.loans() == []
    assert engine.remaining(station_scope("ST1"), "arrival") == 0
    with pytest.raises(RailError):
        engine.repay(loan["id"])
    with pytest.raises(RailError):
        engine.repay("loan-999999")


def test_manual_loan_overdue_is_forced(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, station=1, train=5, repay_mode="manual", grace_periods=1)
    engine.acquire("T01", "ST1", "arrival")
    engine.acquire("T02", "ST1", "arrival")
    clock.advance(10)
    engine.tick()
    assert engine.loans()[0]["status"] == "due"
    clock.advance(10)
    engine.tick()
    assert engine.loans() == []
    forced = [
        settle
        for record in engine.ledger_tail(20)
        if record["op"] == "rollover"
        for settle in record["settles"]
        if settle["mode"] == "forced"
    ]
    assert forced and forced[0]["repaid"] == 1


# ---- 降级 ---------------------------------------------------------------


def test_degrade_cuts_low_priority_first_and_freezes(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, line=10, station=1, train=5)
    engine.acquire("T01", "ST1", "arrival", priority=9)
    engine.acquire("T02", "ST1", "arrival", priority=5)
    engine.acquire("T03", "ST1", "arrival", priority=1)
    assert len(engine.loans()) == 2
    clock.advance(10)
    tick = engine.tick()
    degrades = [s for s in tick["settles"] if s["degrade"]]
    assert len(degrades) == 1 and degrades[0]["degrade"]["shortfall"] == 1
    # 低优先级先砍：T03 被砍，T01/T02 保留。
    assert degrades[0]["degrade"]["cuts"] == ["T03"]
    status = engine.status()
    assert status["degraded"] == ["T03"]
    assert status["credit_frozen"] == [station_scope("ST1")]
    denied = engine.acquire("T03", "ST1", "arrival")
    assert not denied["ok"] and "降级" in denied["reason"]
    frozen = engine.acquire("T04", "ST1", "arrival")
    assert not frozen["ok"] and "信用冻结" in frozen["reason"]
    # 线路的借出已核销，账不能为负。
    assert engine.remaining(line_scope("L1"), "arrival") >= 0
    clock.advance(10)
    engine.tick()
    assert engine.status()["credit_frozen"] == []
    assert engine.acquire("T04", "ST1", "arrival")["ok"]


def test_degrade_cut_tiebreak_by_arrival_order(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, line=10, station=1, train=5)
    engine.acquire("T01", "ST1", "arrival", priority=0)
    engine.acquire("T02", "ST1", "arrival", priority=0)
    engine.acquire("T03", "ST1", "arrival", priority=0)
    clock.advance(10)
    tick = engine.tick()
    cuts = [s["degrade"]["cuts"] for s in tick["settles"] if s["degrade"]]
    # 同优先级按消费顺序先到先砍：T01 最早，先被砍。
    assert cuts == [["T01"]]


# ---- 冲正与部分失败回滚 ---------------------------------------------------


def test_refund_rolls_back_consume_and_loan(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, station=1, train=5)
    engine.acquire("T01", "ST1", "arrival")
    decision = engine.acquire("T02", "ST1", "arrival")
    assert engine.loans()
    refunded = engine.refund(decision["record_id"], reason="对位失败")
    assert refunded["ok"]
    assert engine.loans() == []
    assert engine.remaining(station_scope("ST1"), "arrival") == 0
    assert engine.remaining(line_scope("L1"), "arrival") == 5
    again = engine.refund(decision["record_id"])
    assert not again["ok"] and "已冲正" in again["reason"]


def test_refund_rejected_across_periods(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock)
    decision = engine.acquire("T01", "ST1", "arrival")
    clock.advance(10)
    engine.tick()
    outcome = engine.refund(decision["record_id"])
    assert not outcome["ok"] and "跨周期" in outcome["reason"]


def test_history_is_immutable_refund_is_additive(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock)
    decision = engine.acquire("T01", "ST1", "arrival")
    before = engine.ledger_tail(100)
    consume = [r for r in before if r["id"] == decision["record_id"]][0]
    engine.refund(decision["record_id"])
    after = engine.ledger_tail(100)
    assert len(after) == len(before) + 1
    assert [r for r in after if r["id"] == decision["record_id"]][0] == consume
    assert after[-1]["op"] == "refund" and after[-1]["target"] == decision["record_id"]


# ---- 并发 -----------------------------------------------------------------


def test_concurrent_acquire_never_overcommits(tmp_path):
    clock = ManualClock()
    engine = make_engine(str(tmp_path), clock, line=6, station=2, train=1, period_s=10_000)
    barrier = threading.Barrier(20)
    results: list[dict] = []

    def worker(index: int) -> None:
        barrier.wait()
        results.append(engine.acquire(f"T{index:02d}", "ST1", "arrival"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    granted = [r for r in results if r["ok"]]
    denied = [r for r in results if not r["ok"]]
    assert len(granted) == 6 and len(denied) == 14
    status = engine.status()
    assert status["line"]["arrival"]["used"] == 6
    assert status["stations"]["ST1"]["arrival"]["used"] == 6
    assert len(status["loans"]) == 4  # 站台自有 2 份，其余 4 次靠借
    seqs = sorted(record["seq"] for record in engine.ledger_tail(1000))
    assert seqs == list(range(1, len(seqs) + 1))
    for kinds in engine.status()["stations"].values():
        for state in kinds.values():
            assert state["remaining"] >= 0


# ---- 恢复与崩溃安全 ---------------------------------------------------------


def test_recovery_replays_same_state(tmp_path):
    clock = ManualClock()
    root = str(tmp_path / "state")
    engine = make_engine(root, clock, station=1, train=5)
    engine.acquire("T01", "ST1", "arrival")
    engine.acquire("T02", "ST1", "arrival")
    clock.advance(10)
    engine.tick()
    expected = engine.status()
    rebuilt = make_engine(root, clock, station=1, train=5)
    assert rebuilt.status() == expected
    # 快照损坏或缺失时全量重放，结果一致。
    os.remove(os.path.join(root, "quota", "snapshot.json"))
    replayed = make_engine(root, clock, station=1, train=5)
    assert replayed.status() == expected


def test_torn_tail_is_truncated_on_open(tmp_path):
    clock = ManualClock()
    root = str(tmp_path / "state")
    engine = make_engine(root, clock)
    engine.acquire("T01", "ST1", "arrival")
    records_before = engine.status()["ledger"]["records"]
    segment = os.path.join(root, "quota-00000.jsonl")
    with open(segment, "ab") as handle:
        handle.write(b'{"seq": 999, "op": "cons')
    rebuilt = make_engine(root, clock)
    assert rebuilt.status()["ledger"]["records"] == records_before
    assert rebuilt.acquire("T02", "ST1", "arrival")["ok"]
    again = make_engine(root, clock)
    assert again.remaining(line_scope("L1"), "arrival") == 4


def test_corrupt_middle_segment_raises(tmp_path):
    clock = ManualClock()
    root = str(tmp_path / "state")
    engine = make_engine(root, clock)
    for index in range(10):
        engine.acquire(f"T{index:02d}", "ST1", "arrival")
    segment = os.path.join(root, "quota-00000.jsonl")
    with open(segment, "r+b") as handle:
        handle.seek(3)
        handle.write(b"###")
    with pytest.raises(RailError):
        make_engine(root, clock)


# ---- 平台集成：编排、审计同读一份账 ---------------------------------------


def test_platform_arrive_consumes_hierarchical_quota(tmp_path, clock):
    platform = make_platform(tmp_path, clock)
    platform.start()
    report = platform.arrive("T01", "ST1", 0)
    assert report["ok"]
    quota = platform.status()["quota"]
    assert quota["line"]["arrival"]["used"] == 1
    assert quota["stations"]["ST1"]["arrival"]["used"] == 1
    assert quota["trains"]["T01"]["arrival"]["used"] == 1
    arrival = [e for e in platform.audit.list() if e.kind == "arrival"][0]
    record_id = arrival.detail["quota_record"]
    assert any(r["id"] == record_id for r in platform.quota_records(50))
    assert platform.snapshot()["quota_ledger"]


def test_platform_arrive_failure_refunds_quota(tmp_path, clock):
    platform = make_platform(tmp_path, clock)
    platform.start()
    report = platform.arrive("T01", "ST1", 999)
    assert not report["ok"]
    quota = platform.status()["quota"]
    assert quota["line"]["arrival"]["used"] == 0
    assert quota["stations"]["ST1"]["arrival"]["used"] == 0
    refunds = [r for r in platform.quota_records(50) if r["op"] == "refund"]
    assert refunds and refunds[-1]["reason"]


def test_platform_depart_failure_refunds_quota(tmp_path, clock):
    platform = make_platform(tmp_path, clock)
    platform.start()
    platform.arrive("T01", "ST1", 0)
    platform.check_gap("T01", clear=False)
    report = platform.depart("T01", "ST1")
    assert not report["ok"]
    assert platform.status()["quota"]["line"]["depart"]["used"] == 0
    refunds = [r for r in platform.quota_records(50) if r["op"] == "refund"]
    assert refunds


def test_platform_manual_repay_via_cycle(tmp_path, clock):
    platform = make_platform(
        tmp_path, clock, repay_mode="manual", station_arrival_limit=1, period_s=30.0
    )
    platform.start()
    platform.arrive("T01", "ST1", 0)
    platform.arrive("T02", "ST1", 0)
    loans = platform.quota_engine.loans()
    assert len(loans) == 1
    clock.advance(30)
    platform.cycle()
    assert platform.quota_engine.loans()[0]["status"] == "due"
    outcome = platform.repay_loan(loans[0]["id"])
    assert outcome["ok"] and outcome["repaid"] == 1
    assert platform.quota_engine.loans() == []
    repaid = [e for e in platform.audit.list() if e.kind == "quota_repay"]
    assert repaid and repaid[0].detail["loan_id"] == loans[0]["id"]


def test_platform_quota_denied_when_line_exhausted(tmp_path, clock):
    platform = make_platform(tmp_path, clock, arrival_limit=1, depart_limit=1)
    platform.start()
    assert platform.arrive("T01", "ST1", 0)["ok"]
    report = platform.arrive("T02", "ST2", 0)
    assert not report["ok"] and "线路配额已用完" in report["violations"][0]


def test_console_quota_endpoints(tmp_path, clock):
    from psdline.console.api import Api

    platform = make_platform(tmp_path, clock, station_arrival_limit=1)
    platform.start()
    api = Api(platform)
    platform.arrive("T01", "ST1", 0)
    platform.arrive("T02", "ST1", 0)
    status, quota = api.handle("GET", "/api/quota", {}, {})
    assert status == 200 and quota["line"]["arrival"]["configured"]
    status, loans = api.handle("GET", "/api/quota/loans", {}, {})
    assert status == 200 and len(loans["loans"]) == 1
    status, ledger = api.handle("GET", "/api/quota/ledger", {"limit": "5"}, {})
    assert status == 200 and ledger["records"]
    status, _ = api.handle("POST", "/api/quota/repay", {}, {"loan_id": "loan-999999"})
    assert status == 409
    loan_id = loans["loans"][0]["id"]
    status, repaid = api.handle("POST", "/api/quota/repay", {}, {"loan_id": loan_id})
    assert status == 200 and repaid["ok"]


# ---- 配置 -----------------------------------------------------------------


def test_quota_config_validation():
    payload = config_to_mapping(default_config())
    payload["quota"]["repay_mode"] = "sideways"
    with pytest.raises(ConfigError):
        config_from_mapping(payload)
    payload = config_to_mapping(default_config())
    payload["quota"]["period_s"] = 0
    with pytest.raises(ConfigError):
        config_from_mapping(payload)
    payload = config_to_mapping(default_config())
    payload["quota"]["grace_periods"] = -1
    with pytest.raises(ConfigError):
        config_from_mapping(payload)
    payload = config_to_mapping(default_config())
    payload["quota"]["station_arrival_limit"] = 0
    with pytest.raises(ConfigError):
        config_from_mapping(payload)
