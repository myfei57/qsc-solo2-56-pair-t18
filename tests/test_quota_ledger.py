"""分级配额账本：哈希链、原子事务、崩溃恢复、防篡改。"""

from __future__ import annotations

import json
import os
import threading

import pytest

from psdline.clock import ManualClock
from psdline.errors import StorageError
from psdline.quota import LedgerEvent, QuotaBook, QuotaLedger
from psdline.quota.ledger import GENESIS_HASH, hash_line


@pytest.fixture()
def ledger(tmp_path):
    return QuotaLedger(str(tmp_path / "quota-ledger.jsonl"))


def _event(kind="note", **payload):
    return LedgerEvent(kind, payload)


def test_append_builds_hash_chain(ledger):
    first = ledger.append([_event("a", x=1)], txn_id="t1", at=1.0)
    second = ledger.append([_event("b", x=2)], txn_id="t2", at=2.0)
    assert first.seq == 1 and second.seq == 2
    assert first.prev_hash == GENESIS_HASH
    assert second.prev_hash == first.hash
    assert ledger.verify()["ok"] is True
    assert ledger.verify()["head_seq"] == 2


def test_history_is_immutable_tamper_detected(ledger):
    ledger.append([_event("a", x=1)], txn_id="t1", at=1.0)
    ledger.append([_event("b", x=2)], txn_id="t2", at=2.0)
    path = ledger.path
    with open(path, encoding="utf-8") as handle:
        lines = handle.readlines()
    row = json.loads(lines[0])
    row["events"][0]["payload"]["x"] = 999
    lines[0] = json.dumps(row, sort_keys=True) + "\n"
    with open(path, "w", encoding="utf-8") as handle:
        handle.writelines(lines)
    assert ledger.verify()["ok"] is False
    with pytest.raises(StorageError):
        ledger.read()


def test_torn_tail_is_quarantined_not_silently_dropped(ledger):
    ledger.append([_event("a")], txn_id="t1", at=1.0)
    ledger.append([_event("b")], txn_id="t2", at=2.0)
    with open(ledger.path, "a", encoding="utf-8") as handle:
        handle.write('{"seq": 3, "txn_id": "broken", "events": [{"type": "half')
    recovered = ledger.recover()
    assert recovered["committed"] == 2
    assert recovered["torn_bytes"] > 0
    assert os.path.isfile(ledger.path + ".quarantine")
    # 恢复后可以继续追加，新行序号接着 2 走，链不断。
    third = ledger.append([_event("c")], txn_id="t3", at=3.0)
    assert third.seq == 3
    assert ledger.verify()["ok"] is True


def test_idempotent_retry_does_not_double_append(ledger):
    ledger.append([_event("a")], txn_id="idem-1", at=1.0, idempotency_key="idem-1")
    again = ledger.append(
        [_event("different")], txn_id="idem-1", at=9.0, idempotency_key="idem-1"
    )
    assert again.seq == 1
    txns = ledger.read()
    assert len(txns) == 1
    assert txns[0].events[0].type == "a"


def test_concurrent_appends_are_serialized(tmp_path):
    ledger = QuotaLedger(str(tmp_path / "c.jsonl"))
    errors = []

    def worker(i):
        try:
            ledger.append(
                [_event("n", i=i)],
                txn_id=f"txn-{i}",
                at=float(i),
                idempotency_key=f"txn-{i}",
            )
        except Exception as exc:  # pragma: no cover - 断言用
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    txns = ledger.read()
    assert len(txns) == 50
    assert [txn.seq for txn in txns] == list(range(1, 51))
    assert ledger.verify()["ok"] is True


def test_book_rebuilds_state_from_disk(tmp_path):
    clock = ManualClock()
    path = str(tmp_path / "q.jsonl")
    first = QuotaBook(QuotaLedger(path), clock)
    # 用最小合法建账。
    from psdline.quota import Grant, LEVEL_LINE

    first.initialize([Grant("L1", LEVEL_LINE, 5, 5)])
    first.set_policy(return_mode="manual")
    # 另起实例读同一账本，状态完全由事件重建。
    second = QuotaBook(QuotaLedger(path), ManualClock())
    assert second.state.cycle == 0
    assert second.state.return_mode == "manual"
    assert second.state.entity("L1").account("arrival").granted == 5
