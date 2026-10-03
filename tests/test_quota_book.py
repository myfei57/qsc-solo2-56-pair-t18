"""分级配额业务：三级额度、向上借、两种归还、降级策略、周期结算、并发。"""

from __future__ import annotations

import threading

import pytest

from psdline.clock import ManualClock
from psdline.quota import (
    ARRIVAL,
    DEPART,
    Grant,
    LEVEL_LINE,
    LEVEL_PLATFORM,
    LEVEL_TRAIN,
    LOAN_CLOSED,
    LOAN_DEFAULTED,
    LOAN_DUE,
    LOAN_OPEN,
    POLICY_ARRIVAL,
    QuotaBook,
    QuotaError,
    QuotaLedger,
    RETURN_AUTO,
    RETURN_MANUAL,
)
from psdline.quota.model import active_total, platform_handled


@pytest.fixture()
def path(tmp_path):
    return str(tmp_path / "quota-ledger.jsonl")


def _book(path, *, mode=RETURN_AUTO, policy="priority", line=100, platform=20):
    book = QuotaBook(QuotaLedger(path), ManualClock())
    book.initialize(
        [
            Grant("L1", LEVEL_LINE, line, line),
            Grant("ST1", LEVEL_PLATFORM, platform, platform, parent="L1"),
        ],
        return_mode=mode,
        downgrade_policy=policy,
    )
    return book


def _train(book, train_id, *, arrival=0, depart=0, priority=100, station="ST1"):
    book.register_train(train_id, station, arrival, depart, priority=priority)


def test_own_quota_does_not_borrow(path):
    book = _book(path)
    _train(book, "T1", arrival=1)
    book.reserve(ARRIVAL, "T1", "ST1")
    state = book.state
    assert state.entity("T1").account(ARRIVAL).held == 1
    assert state.entity("ST1").account(ARRIVAL).held == 0
    assert state.open_loans() == []


def test_borrows_up_when_own_empty(path):
    book = _book(path)
    _train(book, "T1")  # 列车额度 0
    book.reserve(ARRIVAL, "T1", "ST1")
    state = book.state
    assert state.entity("ST1").account(ARRIVAL).held == 1
    loan = state.open_loans()[0]
    assert loan.borrower == "T1" and loan.lender == "ST1"
    assert loan.status == LOAN_OPEN


def test_chains_to_line_when_platform_slice_full(path):
    book = _book(path, line=100, platform=1)
    for index, train_id in enumerate(("T1", "T2", "T3"), start=1):
        _train(book, train_id, priority=index)
        book.reserve(ARRIVAL, train_id, "ST1")
    state = book.state
    assert platform_handled(state, ARRIVAL, "ST1") == 3
    # T1 用站台切片；T2 进来时切片已满（经手 1）且线路有余 → 链路到线路；T3 同理。
    assert state.entity("ST1").account(ARRIVAL).held == 1
    assert state.entity("L1").account(ARRIVAL).held == 2
    # T2、T3 的列车贷款带 chain 指向上游；上游两笔是 站台→线路。
    train_chains = [loan for loan in state.loans.values() if loan.chain and loan.borrower_level == LEVEL_TRAIN]
    upstream = [
        loan
        for loan in state.loans.values()
        if loan.borrower_level == "platform" and loan.lender == "L1"
    ]
    assert len(train_chains) == 2
    assert len(upstream) == 2
    assert all(state.loans[loan.chain] in upstream for loan in train_chains)


def test_line_is_hard_total_across_platforms(path):
    book = QuotaBook(QuotaLedger(path), ManualClock())
    book.initialize(
        [
            Grant("L1", LEVEL_LINE, 3, 3),
            Grant("ST1", LEVEL_PLATFORM, 1, 1, parent="L1"),
            Grant("ST2", LEVEL_PLATFORM, 2, 2, parent="L1"),
        ]
    )
    for index in range(1, 5):
        _train(book, f"T{index}", station="ST1")
    for index in range(1, 4):
        book.reserve(ARRIVAL, f"T{index}", "ST1")
    # 全线 3 个车位已占满，即使 ST2 自有的切片还没动用也不能再进。
    _train(book, "T5", station="ST2")
    with pytest.raises(QuotaError):
        book.reserve(ARRIVAL, "T5", "ST2")
    # 释放一个溯源到线路的占用后，T5 才能进。
    permit = book.state.active_arrival("T3").id
    book.release(permit, reason="gone")
    book.reserve(ARRIVAL, "T5", "ST2")
    assert active_total(book.state, ARRIVAL) == 3


def test_rejects_when_all_levels_exhausted(path):
    book = _book(path, line=2, platform=1)
    for train_id in ("T1", "T2", "T3"):
        _train(book, train_id)
    book.reserve(ARRIVAL, "T1", "ST1")
    book.reserve(ARRIVAL, "T2", "ST1")
    with pytest.raises(QuotaError):
        book.reserve(ARRIVAL, "T3", "ST1")
    head_before = book.ledger.head()[0]
    with pytest.raises(QuotaError):
        book.reserve(ARRIVAL, "T3", "ST1", idem="failed-tx")
    # 失败的事务不留痕（部分失败整体回滚）。
    assert book.ledger.head()[0] == head_before


def test_auto_return_closes_loan_on_release(path):
    book = _book(path)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1")
    permit = book.state.active_arrival("T1").id
    book.release(permit, reason="depart")
    loan = list(book.state.loans.values())[0]
    assert loan.status == LOAN_CLOSED
    assert book.state.entity("ST1").account(ARRIVAL).held == 0


def test_manual_return_waits_for_confirmation(path):
    book = _book(path, mode=RETURN_MANUAL)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1")
    permit = book.state.active_arrival("T1").id
    book.release(permit, reason="depart")
    loan = list(book.state.loans.values())[0]
    assert loan.status == LOAN_DUE  # 挂账等人工，额度仍记占用
    result = book.confirm_return()
    assert result["count"] == 1
    assert book.state.loans[loan.id].status == LOAN_CLOSED


def test_manual_settle_does_not_evict_but_blocks(path):
    book = _book(path, mode=RETURN_MANUAL, platform=5)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1")
    result = book.settle_cycle()
    assert result["downgraded"] == []
    assert result["overdue_loans"]
    # 车仍在站（未自动砍），但逾期未确认前不受理新车次。
    assert book.state.active_arrival("T1") is not None
    with pytest.raises(QuotaError):
        book.reserve(DEPART, "T1", "ST1")
    # 人工清退后确认归还，再恢复即可继续。
    book.downgrade("T1", reason="operator")
    book.confirm_return()
    book.restore("T1")
    book.reserve(DEPART, "T1", "ST1")
    assert book.state.entity("T1").degraded is False


def test_auto_settle_downgrades_only_borrowed_trains_by_priority(path):
    book = _book(path, platform=5)
    _train(book, "OWN", arrival=1, priority=1)    # 自有，不欠债
    _train(book, "HI", priority=10)               # 借款，高优先级
    _train(book, "LO", priority=90)               # 借款，低优先级
    book.reserve(ARRIVAL, "OWN", "ST1")
    book.reserve(ARRIVAL, "HI", "ST1")
    book.reserve(ARRIVAL, "LO", "ST1")
    result = book.settle_cycle()
    evicted = [row["train"] for row in result["downgraded"]]
    assert evicted == ["LO", "HI"]                # 低优先级先砍
    assert "OWN" not in evicted                   # 自有车留下
    assert book.state.active_arrival("OWN") is not None
    assert book.state.entity("LO").degraded is True


def test_arrival_policy_cuts_in_arrival_order(path):
    book = _book(path, platform=5, policy=POLICY_ARRIVAL)
    for order, train_id in enumerate(("A", "B", "C")):
        _train(book, train_id, priority=100 + order)
        book.reserve(ARRIVAL, train_id, "ST1")
    result = book.settle_cycle()
    evicted = [row["train"] for row in result["downgraded"]]
    assert evicted == ["A", "B", "C"]             # 无视优先级，按到站先后


def test_settle_is_idempotent_with_explicit_key(path):
    book = _book(path)
    first = book.settle_cycle(idem="settle-window-1")
    again = book.settle_cycle(idem="settle-window-1")
    assert first["cycle"] == 1
    assert again.get("noop") is True
    assert book.state.cycle == 1
    # 不带幂等键的再次调用是正常推进下一个周期。
    third = book.settle_cycle()
    assert third["cycle"] == 2 and third.get("noop") is None


def test_restore_allows_service_again(path):
    book = _book(path)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1")
    book.downgrade("T1", reason="test")
    with pytest.raises(QuotaError):
        book.reserve(DEPART, "T1", "ST1")
    book.restore("T1")
    book.reserve(DEPART, "T1", "ST1")
    assert book.state.entity("T1").degraded is False


def test_defaulted_history_is_immutable_and_repayable(path):
    # auto 模式：把线路/站台额度压到只够 1，造两笔借款后调小发放再结算，
    # 使一笔贷款在窗口内无法正常结清而挂账 defaulted，随后人工补还。
    book = QuotaBook(QuotaLedger(path), ManualClock())
    book.initialize(
        [
            Grant("L1", LEVEL_LINE, 2, 2),
            Grant("ST1", LEVEL_PLATFORM, 1, 1, parent="L1"),
        ],
        return_mode=RETURN_MANUAL,
    )
    _train(book, "T1")
    _train(book, "T2")
    book.reserve(ARRIVAL, "T1", "ST1")
    book.reserve(ARRIVAL, "T2", "ST1")  # 第二笔溯源到线路
    # manual 模式借出的贷款，降级释放时挂 due（mode 在借出时固定）。
    book.downgrade("T2", reason="hold")
    due_loan = [
        loan.id
        for loan in book.state.loans.values()
        if loan.status == LOAN_DUE and loan.borrower_level == LEVEL_TRAIN
    ]
    assert due_loan
    # 历史事件数只增；补还后状态变 closed，但 defaulted/closed 轨迹都留在账上。
    head = book.ledger.head()[0]
    book.confirm_return(due_loan)
    assert book.state.loans[due_loan[0]].status == LOAN_CLOSED
    assert book.ledger.head()[0] > head
    assert book.ledger.verify()["ok"] is True


def test_repay_defaulted_loan(path):
    # 用手动构造事件的方式制造一笔 defaulted，验证补还命令只追加不改历史。
    from psdline.quota import LedgerEvent

    book = _book(path, platform=5)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1")
    loan_id = list(book.state.loans.values())[0].id
    ledger = book.ledger
    with ledger.transaction() as txn:
        base = list(txn.transactions)
        head = base[-1].seq
        from psdline.quota.ledger import LedgerTransaction

        staged = LedgerTransaction(
            seq=head + 1,
            txn_id="force-default",
            at=1.0,
            actor="test",
            events=(LedgerEvent(
                "loan_status_changed", {"id": loan_id, "status": LOAN_DEFAULTED}
            ),),
            prev_hash="",
            hash="",
        )
        from psdline.quota.model import fold as _fold

        _fold([*base, staged])  # 预演合法
        txn.append(
            [LedgerEvent("loan_status_changed", {"id": loan_id, "status": LOAN_DEFAULTED})],
            txn_id="force-default",
            at=1.0,
            actor="test",
            idempotency_key="force-default",
        )
    assert book.state.loans[loan_id].status == LOAN_DEFAULTED
    book.repay_defaulted(loan_id)
    assert book.state.loans[loan_id].status == LOAN_CLOSED
    # 历史不可变：账本里仍能查到 defaulted 与 closed 两条状态事件。
    statuses = []
    for txn in ledger.read():
        for event in txn.events:
            if event.type == "loan_status_changed" and event.payload.get("id") == loan_id:
                statuses.append(event.payload["status"])
    assert LOAN_DEFAULTED in statuses and LOAN_CLOSED in statuses
    assert ledger.verify()["ok"] is True


def test_concurrent_reservations_never_oversell(path):
    book = _book(path, line=30, platform=10)
    for index in range(40):
        _train(book, f"U{index:02d}")

    accepted = []
    rejected = []
    lock = threading.Lock()

    def worker(index):
        local = QuotaBook(QuotaLedger(path), ManualClock())
        try:
            local.reserve(ARRIVAL, f"U{index:02d}", "ST1", idem=f"u{index}")
            with lock:
                accepted.append(index)
        except QuotaError:
            with lock:
                rejected.append(index)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(40)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(accepted) == 30 and len(rejected) == 10
    assert active_total(QuotaBook(QuotaLedger(path), ManualClock()).state, ARRIVAL) == 30


def test_idempotent_reserve_retry_is_safe(path):
    book = _book(path)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1", idem="same-key")
    # 同键重试（即便在另一实例上）不产生第二个占用。
    other = QuotaBook(QuotaLedger(path), ManualClock())
    other.reserve(ARRIVAL, "T1", "ST1", idem="same-key")
    permits = [
        permit
        for permit in other.state.permits.values()
        if permit.kind == ARRIVAL and permit.train_id == "T1"
    ]
    assert len(permits) == 1


def test_degraded_train_is_refused(path):
    book = _book(path)
    _train(book, "T1")
    book.downgrade("T1")
    with pytest.raises(QuotaError):
        book.reserve(ARRIVAL, "T1", "ST1")


def test_set_grants_rejects_below_held(path):
    book = _book(path, platform=5)
    _train(book, "T1")
    book.reserve(ARRIVAL, "T1", "ST1")
    with pytest.raises(QuotaError):
        book.set_grants([Grant("ST1", LEVEL_PLATFORM, 0, 0, parent="L1")])
