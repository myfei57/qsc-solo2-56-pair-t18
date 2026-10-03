"""分级运行配额的只追加账本。

账本是整个配额子系统唯一的事实来源（single source of truth）：

* 每一行是一笔**事务**，事务里带一个或多个事件；事务要么整行落盘（提交），
  要么一行都不留（回滚），不存在"提交了一半"的状态。
* 行与行之间用 SHA-256 串成哈希链：``prev_hash`` 指向上一行的 ``hash``，任何
  一行被删改都会让后续所有校验失败。历史不可变——修正只能靠追加补偿事件。
* 写入流程：文件锁串行化跨进程并发 → 取上一行哈希 → ``fsync`` 落盘。崩溃若
  留下最后一行半截 JSON，加载时隔离（quarantine）后从断点继续，绝不静默吞掉。
* 事务携带 ``txn_id``（默认按调用方给定的幂等键生成），同一进程重放同一键是
  无副作用的，支持调用方失败重试。

配额台账（projection）、到发编排、审计都从 :meth:`QuotaLedger.read` 读出的
同一序列派生，因此三者读到的永远是同一份账。
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Iterator

from ..errors import StorageError

GENESIS_HASH = "0" * 64
QUARANTINE_SUFFIX = ".quarantine"


@dataclass(frozen=True)
class LedgerEvent:
    """事务里的一个事件，``type`` 决定 projection 如何折叠。"""

    type: str
    payload: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"type": self.type, "payload": dict(self.payload)}

    @classmethod
    def from_dict(cls, raw: dict) -> "LedgerEvent":
        payload = raw.get("payload", {})
        return cls(
            type=str(raw.get("type", "")),
            payload=dict(payload) if isinstance(payload, dict) else {},
        )


@dataclass(frozen=True)
class LedgerTransaction:
    """账本里的一行：一个不可分割的提交单位。"""

    seq: int
    txn_id: str
    at: float
    actor: str
    events: tuple[LedgerEvent, ...]
    prev_hash: str
    hash: str

    def as_line(self) -> str:
        """序列化成落盘的一行（不含行尾换行）。"""

        body = {
            "seq": self.seq,
            "txn_id": self.txn_id,
            "at": self.at,
            "actor": self.actor,
            "prev_hash": self.prev_hash,
            "events": [event.as_dict() for event in self.events],
        }
        return json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_line(cls, line: str) -> "LedgerTransaction":
        raw = json.loads(line)
        events = tuple(
            LedgerEvent.from_dict(item)
            for item in raw.get("events", [])
            if isinstance(item, dict)
        )
        return cls(
            seq=int(raw["seq"]),
            txn_id=str(raw.get("txn_id", "")),
            at=float(raw.get("at", 0.0)),
            actor=str(raw.get("actor", "")),
            events=events,
            prev_hash=str(raw.get("prev_hash", GENESIS_HASH)),
            hash=str(raw.get("hash", "")),
        )


def hash_line(canonical_line: str, prev_hash: str) -> str:
    """计算一行的哈希：上一行哈希与本行规范 JSON 的 SHA-256。"""

    digest = hashlib.sha256()
    digest.update(prev_hash.encode("ascii"))
    digest.update(b"\n")
    digest.update(canonical_line.encode("utf-8"))
    return digest.hexdigest()


class QuotaLedger:
    """单个 JSONL 文件上的只追加事务账本。"""

    def __init__(self, path: str) -> None:
        self._path = os.path.abspath(path)
        self._lock_path = self._path + ".lock"
        self._thread_lock = threading.RLock()
        directory = os.path.dirname(self._path)
        os.makedirs(directory, exist_ok=True)
        # 锁文件长期存在；它不参与账本内容，只是跨进程互斥的锚点。
        with open(self._lock_path, "a", encoding="utf-8"):
            pass

    @property
    def path(self) -> str:
        return self._path

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        """进程内可重入锁 + 跨进程排他文件锁。"""

        with self._thread_lock:
            with open(self._lock_path, "r", encoding="utf-8") as lock_handle:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    @contextlib.contextmanager
    def transaction(self) -> Iterator["LedgerTxn"]:
        """打开一个持锁事务上下文。

        进入时持有跨进程排他锁并读出当前全部已提交事务；在上下文内
        :meth:`LedgerTxn.append` 用同一把锁、同一个链头续写。业务的
        "读台账 → 校验 → 追加"必须整体包在这一个上下文里，否则并发命令会各自
        基于过期状态决策而超卖。
        """

        with self._thread_lock:
            with open(self._lock_path, "r", encoding="utf-8") as lock_handle:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                txn: LedgerTxn | None = None
                try:
                    transactions, torn, prev_hash = self._load_valid()
                    torn_bytes = len(torn.encode("utf-8")) if torn else 0
                    if torn is not None:
                        quarantine_path = self._quarantine(torn)
                        transactions, _torn, prev_hash = self._load_valid()
                    else:
                        quarantine_path = ""
                    txn = LedgerTxn(
                        self, transactions, prev_hash,
                        recovered_bytes=torn_bytes,
                        quarantine_path=quarantine_path,
                    )
                    yield txn
                finally:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def _load_valid(self) -> tuple[list[LedgerTransaction], str | None, str]:
        """读入全部完整合法的事务行。

        返回 ``(事务列表, 被隔离的原始末行或 None, 最后一行哈希)``。中间任何一
        行损坏（缺字段、哈希断裂、JSON 错误且不在文件末尾）都属于有人动过账，
        直接抛 :class:`StorageError`，不允许带着破账继续跑。
        """

        if not os.path.isfile(self._path):
            return [], None, GENESIS_HASH

        with open(self._path, "r", encoding="utf-8") as handle:
            raw_lines = handle.readlines()

        transactions: list[LedgerTransaction] = []
        prev_hash = GENESIS_HASH
        last_nonempty = -1
        for index, raw in enumerate(raw_lines):
            if raw.strip():
                last_nonempty = index

        torn: str | None = None
        for index, raw in enumerate(raw_lines):
            if not raw.strip():
                continue
            try:
                txn = LedgerTransaction.from_line(raw)
            except (ValueError, TypeError, json.JSONDecodeError, KeyError) as error:
                if index == last_nonempty:
                    torn = raw
                    break
                raise StorageError(
                    f"配额账本第 {index + 1} 行损坏: {error}", component="quota"
                ) from error
            expected = hash_line(
                txn.as_line().strip(), txn.prev_hash
            )
            if txn.prev_hash != prev_hash:
                raise StorageError(
                    f"配额账本第 {txn.seq} 行哈希链断裂：前链不匹配", component="quota"
                )
            if txn.hash and txn.hash != expected:
                raise StorageError(
                    f"配额账本第 {txn.seq} 行哈希不匹配，账本可能被篡改",
                    component="quota",
                )
            transactions.append(txn)
            prev_hash = txn.hash or expected
        return transactions, torn, prev_hash

    def _quarantine(self, torn: str) -> str:
        """把半截末行挪到隔离文件，绝不静默丢弃。"""

        target = self._path + QUARANTINE_SUFFIX
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(torn if torn.endswith("\n") else torn + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        with open(self._path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
        # torn 一定是最后一个非空行；只保留它之前的完整行。
        kept = lines[:-1]
        tmp_path = self._path + ".repair"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.writelines(kept)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self._path)
        return target

    def recover(self) -> dict:
        """启动/运维期恢复：事务上下文自动隔离半截末行，返回恢复结果。"""

        with self.transaction() as txn:
            return {
                "ok": True,
                "committed": len(txn.transactions),
                "torn_bytes": txn.recovered_bytes,
                "quarantined_to": txn.quarantine_path,
            }

    def read(self) -> list[LedgerTransaction]:
        """读出全部已提交事务（按序号升序）；顺带隔离半截末行。"""

        with self.transaction() as txn:
            return list(txn.transactions)

    def head(self) -> tuple[int, str]:
        """返回 ``(最后序号, 最后一行哈希)``，空账本为 ``(0, 创世哈希)``。"""

        with self.transaction() as txn:
            return txn.head_seq, txn.prev_hash

    def append(
        self,
        events: list[LedgerEvent] | list[dict[str, Any]],
        *,
        txn_id: str,
        at: float,
        actor: str = "system",
        idempotency_key: str = "",
    ) -> LedgerTransaction:
        """追加一笔事务（便捷入口，内部走 :meth:`transaction` 临界区）。

        ``idempotency_key`` 命中历史事务时原样返回该事务，不重复追加——调用方
        超时重试是安全的。
        """

        with self.transaction() as txn:
            return txn.append(
                events, txn_id=txn_id, at=at, actor=actor,
                idempotency_key=idempotency_key,
            )

    def verify(self) -> dict:
        """从头到尾重算哈希链，供审计巡检调用。

        与 :meth:`read` 不同：巡检只报告 ``ok: False`` 与坏行序号，不抛异常，
        以便控制台/巡检接口把"账本被篡改"作为可读结果暴露出来。
        """

        with self._locked():
            if not os.path.isfile(self._path):
                return {"ok": True, "committed": 0, "torn_pending": False, "head_seq": 0}
            with open(self._path, "r", encoding="utf-8") as handle:
                raw_lines = [line for line in handle.readlines() if line.strip()]
            prev_hash = GENESIS_HASH
            committed = 0
            torn_pending = False
            for index, raw in enumerate(raw_lines, start=1):
                try:
                    txn = LedgerTransaction.from_line(raw)
                except (ValueError, TypeError, json.JSONDecodeError, KeyError):
                    if index == len(raw_lines):
                        torn_pending = True
                        break
                    return {"ok": False, "bad_seq": index, "committed": committed}
                expected = hash_line(txn.as_line().strip(), txn.prev_hash)
                if txn.prev_hash != prev_hash or (txn.hash and txn.hash != expected):
                    return {"ok": False, "bad_seq": txn.seq, "committed": committed}
                prev_hash = txn.hash or expected
                committed += 1
            return {
                "ok": True,
                "committed": committed,
                "torn_pending": torn_pending,
                "head_seq": committed,
            }


class LedgerTxn:
    """持锁事务句柄：在临界区内读最新状态、续写事务。"""

    def __init__(
        self,
        ledger: "QuotaLedger",
        transactions: list[LedgerTransaction],
        prev_hash: str,
        *,
        recovered_bytes: int = 0,
        quarantine_path: str = "",
    ) -> None:
        self._ledger = ledger
        self.transactions = transactions
        self.prev_hash = prev_hash
        self.recovered_bytes = recovered_bytes
        self.quarantine_path = quarantine_path

    @property
    def head_seq(self) -> int:
        return self.transactions[-1].seq if self.transactions else 0

    def find(self, key: str) -> LedgerTransaction | None:
        """按 ``txn_id`` 检索历史事务（幂等）。"""

        if not key:
            return None
        for txn in self.transactions:
            if txn.txn_id == key:
                return txn
        return None

    def append(
        self,
        events: list[LedgerEvent] | list[dict[str, Any]],
        *,
        txn_id: str,
        at: float,
        actor: str = "system",
        idempotency_key: str = "",
    ) -> LedgerTransaction:
        """在本事务临界区内追加一笔并 ``fsync``。"""

        key = idempotency_key or txn_id
        existing = self.find(key)
        if existing is not None:
            return existing
        if not events:
            raise StorageError("空事务不能落账", component="quota")
        normalized = tuple(
            event
            if isinstance(event, LedgerEvent)
            else LedgerEvent(
                type=str(event["type"]), payload=dict(event.get("payload", {}))
            )
            for event in events
        )
        seq = self.head_seq + 1
        draft = LedgerTransaction(
            seq=seq,
            txn_id=txn_id,
            at=at,
            actor=actor,
            events=normalized,
            prev_hash=self.prev_hash,
            hash="",
        )
        canonical = draft.as_line()
        digest = hash_line(canonical, self.prev_hash)
        committed = LedgerTransaction(
            seq=seq,
            txn_id=txn_id,
            at=at,
            actor=actor,
            events=normalized,
            prev_hash=self.prev_hash,
            hash=digest,
        )
        line_dict = json.loads(canonical)
        line_dict["hash"] = digest
        line = json.dumps(
            line_dict, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        try:
            with open(self._ledger.path, "a", encoding="utf-8") as handle:
                handle.write(line + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        except OSError as error:
            raise StorageError(f"追加配额账本失败: {error}", component="quota") from error
        self.transactions.append(committed)
        self.prev_hash = digest
        return committed
