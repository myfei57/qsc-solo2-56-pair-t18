"""分级配额引擎：层级份额、向上借、到期归还与降级。

规则一句话：列车、站台、线路三级各持一份到发额度，消费一次三级同时扣；下级
不够时向上级借（借据下个周期到期），到期从下级新周期份额里扣还，还不上就
降级——冻结下级本期全部份额与其信用（本期不得再借），并按「低优先级优先、
同级先到先砍」砍掉本期已在该级消费过的列车，被砍列车本周期不再放行。

一致性：所有变动先落账本（:class:`~psdline.quota.ledger.QuotaLedger`）再改
内存，检查—追加—应用在同一把锁里完成，并发借还不会超扣；编排失败用
:meth:`QuotaEngine.refund` 追加冲正记录回滚，历史不可变。崩溃后按快照加流水
尾部重放，恢复出同一份账。
"""

from __future__ import annotations

import threading

from ..clock import Clock
from ..errors import RailError, StorageError
from .ledger import QuotaLedger
from .scopes import (
    AUTO,
    FORCED,
    KINDS,
    LEVELS,
    LOAN_DUE,
    LOAN_OPEN,
    LOAN_REFUNDED,
    LOAN_REPAID,
    LOAN_DEGRADED,
    LOAN_TERMINAL,
    MANUAL,
    REPAY_MODES,
    KindState,
    Loan,
    level_of,
    line_scope,
    name_of,
    station_scope,
    train_scope,
)


class QuotaEngine:
    """三级配额的借还、清算与降级引擎。"""

    def __init__(
        self,
        ledger: QuotaLedger,
        clock: Clock,
        *,
        line_id: str,
        period_s: float,
        repay_mode: str,
        grace_periods: int,
        limits: dict[tuple[str, str], int],
    ) -> None:
        if not line_id:
            raise RailError("线路标识不能为空", component="quota")
        if period_s <= 0:
            raise RailError("配额周期必须为正", component="quota")
        if repay_mode not in REPAY_MODES:
            raise RailError(f"未知归还方式: {repay_mode}", component="quota")
        if grace_periods < 0:
            raise RailError("宽限周期数不能为负", component="quota")
        for level in LEVELS:
            for kind in KINDS:
                if int(limits.get((level, kind), 0)) <= 0:
                    raise RailError(f"配额份额缺失或非正: {level}/{kind}", component="quota")
        self._ledger = ledger
        self._clock = clock
        self._line_id = line_id
        self._period_s = float(period_s)
        self._repay_mode = repay_mode
        self._grace_periods = int(grace_periods)
        self._limits = {(level, kind): int(value) for (level, kind), value in limits.items()}
        self._lock = threading.RLock()
        self._period = -1
        self._scopes: dict[str, dict[str, KindState]] = {}
        self._loans: dict[str, Loan] = {}
        self._loan_seq = 0
        self._degraded: set[str] = set()
        self._frozen: set[str] = set()
        self._consumers: dict[str, list[dict]] = {}
        self._live: dict[str, dict] = {}
        self._snapshot_errors = 0
        self._recover()

    # ---- 恢复 ---------------------------------------------------------

    def _recover(self) -> None:
        """快照加流水尾部重放；快照缺失或损坏时全量重放，结果一致。"""

        base = 0
        snapshot = self._ledger.load_snapshot()
        if snapshot is not None:
            base, state = snapshot
            self._load_state(state)
        records = self._ledger.records()
        for record in records:
            if int(record.get("seq", 0)) <= base:
                continue
            self._apply(record)
        if self._period < 0 and records:
            self._period = max(int(record.get("period", 0)) for record in records)
        self._verify()

    def _verify(self) -> None:
        """回放后的不变量检查：任何份额计数都不允许为负。"""

        for scope, kinds in self._scopes.items():
            for kind, state in kinds.items():
                if state.used < 0 or state.lent < 0 or state.borrowed < 0:
                    raise StorageError(f"账本回放后出现负值: {scope}/{kind}", component="quota")

    def _state_dict(self) -> dict:
        return {
            "period": self._period,
            "loan_seq": self._loan_seq,
            "scopes": {
                scope: {kind: state.as_dict() for kind, state in kinds.items()}
                for scope, kinds in self._scopes.items()
            },
            "loans": {loan_id: loan.as_dict() for loan_id, loan in self._loans.items()},
            "degraded": sorted(self._degraded),
            "frozen": sorted(self._frozen),
            "consumers": {
                scope: [dict(entry) for entry in entries]
                for scope, entries in self._consumers.items()
            },
            "live": {record_id: dict(meta) for record_id, meta in self._live.items()},
        }

    def _load_state(self, state: dict) -> None:
        self._period = int(state.get("period", -1))
        self._loan_seq = int(state.get("loan_seq", 0))
        self._scopes = {
            str(scope): {
                str(kind): KindState.from_dict(payload)
                for kind, payload in dict(kinds).items()
                if isinstance(payload, dict)
            }
            for scope, kinds in dict(state.get("scopes", {})).items()
            if isinstance(kinds, dict)
        }
        self._loans = {
            str(loan_id): Loan.from_dict(payload)
            for loan_id, payload in dict(state.get("loans", {})).items()
            if isinstance(payload, dict)
        }
        self._degraded = {str(item) for item in state.get("degraded", [])}
        self._frozen = {str(item) for item in state.get("frozen", [])}
        self._consumers = {
            str(scope): [dict(entry) for entry in entries if isinstance(entry, dict)]
            for scope, entries in dict(state.get("consumers", {})).items()
            if isinstance(entries, list)
        }
        self._live = {
            str(record_id): dict(meta)
            for record_id, meta in dict(state.get("live", {})).items()
            if isinstance(meta, dict)
        }

    def _save_snapshot(self) -> None:
        """落检查点；失败只计数不阻断——快照只是加速恢复，账本才是事实。"""

        try:
            self._ledger.save_snapshot(self._state_dict())
        except StorageError:
            self._snapshot_errors += 1

    # ---- 周期 ---------------------------------------------------------

    def _ensure_period(self) -> dict | None:
        """按钟面对齐周期；跨周期时先结算到期借据再翻篇，返回翻篇记录。"""

        target = int(self._clock.now() // self._period_s)
        if self._period < 0:
            self._period = target
            return None
        if target > self._period:
            return self._rollover(target)
        return None

    def _rollover(self, target: int) -> dict:
        """翻篇：到期自动借据立即清算，人工借据转到期或逾期强制清算。"""

        settles: list[dict] = []
        due: list[str] = []
        holds: dict[tuple[str, str], int] = {}
        for loan in sorted(self._loans.values(), key=lambda item: item.id):
            if loan.status not in (LOAN_OPEN, LOAN_DUE) or loan.due_period > target:
                continue
            if loan.mode == MANUAL and target - loan.due_period < self._grace_periods:
                if loan.status == LOAN_OPEN:
                    due.append(loan.id)
                continue
            mode = AUTO if loan.mode == AUTO else FORCED
            settles.append(self._plan_settle(loan, mode, holds, fresh=True))
        record = self._ledger.append(
            "rollover",
            target,
            **{"from": self._period, "to": target, "due": due, "settles": settles},
        )
        self._apply(record)
        self._save_snapshot()
        return record

    # ---- 借还 ---------------------------------------------------------

    def configure(self, scope: str, kind: str, limit: int) -> dict:
        """配置（或调整）一个作用域的周期份额。"""

        if kind not in KINDS:
            raise RailError(f"未知到发类别: {kind}", component="quota")
        if limit <= 0:
            raise RailError("配额上限必须为正", component="quota")
        with self._lock:
            self._ensure_period()
            record = self._ledger.append(
                "configure", self._period, scope=scope, kind=kind, limit=int(limit)
            )
            self._apply(record)
            self._save_snapshot()
            return {"ok": True, "record_id": record["id"], "scope": scope, "kind": kind}

    def acquire(
        self,
        train_id: str,
        station_id: str,
        kind: str,
        priority: int = 0,
        repay_mode: str | None = None,
    ) -> dict:
        """为一次到发申请配额：三级同时扣减，不够就向上借，借不到就拒。"""

        if kind not in KINDS:
            raise RailError(f"未知到发类别: {kind}", component="quota")
        mode = repay_mode or self._repay_mode
        if mode not in REPAY_MODES:
            raise RailError(f"未知归还方式: {mode}", component="quota")
        with self._lock:
            self._ensure_period()
            chain = [train_scope(train_id), station_scope(station_id), line_scope(self._line_id)]
            for scope in chain:
                self._ensure_scope(scope)
            if train_id in self._degraded:
                return self._deny(train_id, station_id, kind, priority, "配额降级，本周期不再放行")
            loans: list[dict] = []
            # 借据随消费单一次落账，循环内先用 pending 记账让缺口判断收敛。
            pending: dict[str, int] = {}
            while True:
                short = None
                for scope in reversed(chain):
                    state = self._kind_state(scope, kind)
                    if state.remaining + pending.get(scope, 0) <= 0:
                        short = scope
                        break
                if short is None:
                    break
                if short == chain[-1]:
                    return self._deny(train_id, station_id, kind, priority, f"线路配额已用完: {kind}")
                if short in self._frozen:
                    return self._deny(
                        train_id, station_id, kind, priority, f"配额降级，本周期信用冻结: {short}"
                    )
                lender = chain[chain.index(short) + 1]
                lender_state = self._kind_state(lender, kind)
                if lender_state.remaining + pending.get(lender, 0) < 1:
                    return self._deny(
                        train_id,
                        station_id,
                        kind,
                        priority,
                        f"配额不足且上级无余量可借: {short}",
                    )
                loans.append(self._open_loan(short, lender, kind, mode))
                pending[short] = pending.get(short, 0) + 1
            record = self._ledger.append(
                "consume",
                self._period,
                train_id=train_id,
                station_id=station_id,
                kind=kind,
                priority=int(priority),
                chain=chain,
                loans=loans,
            )
            self._apply(record)
            self._save_snapshot()
            return {
                "ok": True,
                "record_id": record["id"],
                "loans": [loan["id"] for loan in loans],
                "reason": "",
            }

    def refund(self, record_id: str, reason: str = "") -> dict:
        """冲正一笔本周期的消费（编排失败回滚），连带回笼随单借出的份额。"""

        with self._lock:
            self._ensure_period()
            live = self._live.get(record_id)
            if live is None:
                return {"ok": False, "record_id": record_id, "reason": f"消费记录不存在或已冲正: {record_id}"}
            if int(live.get("period", -1)) != self._period:
                return {"ok": False, "record_id": record_id, "reason": "跨周期冲正已失效"}
            record = self._ledger.append(
                "refund", self._period, target=record_id, reason=reason or "编排失败回滚"
            )
            self._apply(record)
            self._save_snapshot()
            return {"ok": True, "record_id": record["id"], "refunded": record_id}

    def repay(self, loan_id: str) -> dict:
        """人工确认归还一笔借据；份额不够还的部分当场降级。"""

        with self._lock:
            self._ensure_period()
            loan = self._loans.get(loan_id)
            if loan is None:
                raise RailError(f"借据不存在: {loan_id}", component="quota")
            if loan.status in LOAN_TERMINAL:
                raise RailError(f"借据已结清: {loan_id}", component="quota")
            payload = self._plan_settle(loan, MANUAL, {}, fresh=False)
            record = self._ledger.append("settle", self._period, **payload)
            self._apply(record)
            self._save_snapshot()
            return {
                "ok": True,
                "record_id": record["id"],
                "loan_id": loan_id,
                "repaid": payload["repaid"],
                "degrade": payload["degrade"],
            }

    def tick(self) -> dict:
        """控制循环入口：对齐周期并返回本期清算摘要。"""

        with self._lock:
            before = self._period
            rolled = self._ensure_period()
            return {
                "ok": True,
                "period": self._period,
                "rolled": self._period != before,
                "settles": list(rolled.get("settles", [])) if rolled else [],
                "loans_open": len(self.open_loans()),
                "degraded": sorted(self._degraded),
            }

    # ---- 查询 ---------------------------------------------------------

    def remaining(self, scope: str, kind: str) -> int:
        """返回一个作用域本期剩余份额。"""

        with self._lock:
            kinds = self._scopes.get(scope)
            if kinds is None or kind not in kinds:
                raise RailError(f"配额未配置: {scope}/{kind}", component="quota")
            return kinds[kind].remaining

    def open_loans(self) -> list[Loan]:
        """返回全部未结清借据（在途 + 到期待还）。"""

        with self._lock:
            return sorted(
                (loan for loan in self._loans.values() if loan.status not in LOAN_TERMINAL),
                key=lambda loan: loan.id,
            )

    def loans(self) -> list[dict]:
        """返回未结清借据的字典视图，供控制台与审计读取。"""

        return [loan.as_dict() for loan in self.open_loans()]

    def ledger_tail(self, limit: int = 50) -> list[dict]:
        """返回账本尾部记录——审计读的同一份账。"""

        return self._ledger.tail(limit)

    def status(self) -> dict:
        """返回分级配额状态快照。"""

        with self._lock:
            stations: dict[str, dict] = {}
            trains: dict[str, dict] = {}
            for scope, kinds in sorted(self._scopes.items()):
                view = {
                    kind: {"configured": True, **state.as_dict()}
                    for kind, state in sorted(kinds.items())
                }
                level = level_of(scope)
                if level == "station":
                    stations[name_of(scope)] = view
                elif level == "train":
                    trains[name_of(scope)] = view
            line_kinds = self._scopes.get(line_scope(self._line_id), {})
            line_view = {
                kind: (
                    {"configured": True, **line_kinds[kind].as_dict()}
                    if kind in line_kinds
                    else {"configured": False}
                )
                for kind in KINDS
            }
            return {
                "period": self._period,
                "period_s": self._period_s,
                "repay_mode": self._repay_mode,
                "grace_periods": self._grace_periods,
                "line": line_view,
                "stations": stations,
                "trains": trains,
                "loans": self.loans(),
                "loans_settled": sum(
                    1 for loan in self._loans.values() if loan.status in LOAN_TERMINAL
                ),
                "degraded": sorted(self._degraded),
                "credit_frozen": sorted(self._frozen),
                "ledger": self._ledger.stats(),
                "snapshot_errors": self._snapshot_errors,
            }

    # ---- 内部 ---------------------------------------------------------

    def _kind_state(self, scope: str, kind: str) -> KindState:
        kinds = self._scopes.setdefault(scope, {})
        if kind not in kinds:
            kinds[kind] = KindState()
        return kinds[kind]

    def _ensure_scope(self, scope: str) -> None:
        """作用域缺失时按默认份额补配置记录（列车首次出现时用到）。"""

        for kind in KINDS:
            if kind in self._scopes.get(scope, {}):
                continue
            limit = self._limits[(level_of(scope), kind)]
            record = self._ledger.append(
                "configure", self._period, scope=scope, kind=kind, limit=limit
            )
            self._apply(record)

    def _open_loan(self, borrower: str, lender: str, kind: str, mode: str) -> dict:
        self._loan_seq += 1
        return {
            "id": f"loan-{self._loan_seq:06d}",
            "borrower": borrower,
            "lender": lender,
            "kind": kind,
            "amount": 1,
            "mode": mode,
            "created_period": self._period,
            "due_period": self._period + 1,
            "created_at": self._clock.now(),
        }

    def _deny(self, train_id: str, station_id: str, kind: str, priority: int, reason: str) -> dict:
        """拒绝也落账：谁、在哪、因为什么没拿到配额，审计要能查到。"""

        record = self._ledger.append(
            "deny",
            self._period,
            train_id=train_id,
            station_id=station_id,
            kind=kind,
            priority=int(priority),
            reason=reason,
        )
        return {"ok": False, "record_id": record["id"], "loans": [], "reason": reason}

    def _plan_settle(
        self,
        loan: Loan,
        mode: str,
        holds: dict[tuple[str, str], int],
        fresh: bool,
    ) -> dict:
        """算一笔借据的清算方案：还得起的部分冻结抵债，还不起的部分降级。"""

        state = self._kind_state(loan.borrower, loan.kind)
        held = holds.get((loan.borrower, loan.kind), 0)
        # 翻篇清算按新周期账算（used 与 debt_hold 刚清零）；人工确认按当前账算。
        frozen = held if fresh else state.debt_hold + held
        used = 0 if fresh else state.used
        capacity = max(0, state.limit - frozen - used)
        repaid = min(loan.open_amount, capacity)
        degrade = None
        shortfall = loan.open_amount - repaid
        if shortfall > 0:
            degrade = {"shortfall": shortfall, "cuts": self._pick_cuts(loan.borrower, shortfall)}
        holds[(loan.borrower, loan.kind)] = held + repaid
        return {"loan_id": loan.id, "repaid": repaid, "mode": mode, "degrade": degrade}

    def _pick_cuts(self, scope: str, count: int) -> list[str]:
        """降级砍单：低优先级列车先砍，同优先级按消费顺序先到先砍。"""

        entries = sorted(
            self._consumers.get(scope, []),
            key=lambda item: (int(item.get("priority", 0)), int(item.get("seq", 0))),
        )
        picked: list[str] = []
        for entry in entries:
            train_id = str(entry.get("train_id", ""))
            if not train_id or train_id in picked:
                continue
            picked.append(train_id)
            if len(picked) >= count:
                break
        return picked

    # ---- 回放 ---------------------------------------------------------

    def _apply(self, record: dict) -> None:
        op = record.get("op")
        if op == "configure":
            self._kind_state(str(record["scope"]), str(record["kind"])).limit = int(record["limit"])
        elif op == "consume":
            self._apply_consume(record)
        elif op == "deny":
            pass
        elif op == "refund":
            self._apply_refund(record)
        elif op == "settle":
            self._apply_settle(record)
        elif op == "rollover":
            self._apply_rollover(record)
        else:
            raise StorageError(f"账本记录类型未知: {op}", component="quota")

    def _apply_consume(self, record: dict) -> None:
        kind = str(record["kind"])
        chain = [str(scope) for scope in record.get("chain", [])]
        for scope in chain:
            self._kind_state(scope, kind).used += 1
        loan_ids: list[str] = []
        for payload in record.get("loans", []):
            loan = Loan(
                id=str(payload["id"]),
                borrower=str(payload["borrower"]),
                lender=str(payload["lender"]),
                kind=str(payload["kind"]),
                amount=int(payload["amount"]),
                open_amount=int(payload["amount"]),
                mode=str(payload["mode"]),
                status=LOAN_OPEN,
                created_period=int(payload["created_period"]),
                due_period=int(payload["due_period"]),
                created_at=float(payload["created_at"]),
            )
            self._loans[loan.id] = loan
            self._kind_state(loan.lender, loan.kind).lent += loan.amount
            self._kind_state(loan.borrower, loan.kind).borrowed += loan.amount
            loan_ids.append(loan.id)
        entry = {
            "seq": int(record.get("seq", 0)),
            "train_id": str(record.get("train_id", "")),
            "priority": int(record.get("priority", 0)),
        }
        for scope in chain:
            self._consumers.setdefault(scope, []).append(entry)
        self._live[str(record["id"])] = {
            "chain": chain,
            "kind": kind,
            "period": int(record.get("period", 0)),
            "loans": loan_ids,
        }

    def _apply_refund(self, record: dict) -> None:
        live = self._live.pop(str(record.get("target", "")), None)
        if live is None:
            return
        kind = str(live["kind"])
        for scope in live["chain"]:
            self._kind_state(scope, kind).used -= 1
        for loan_id in live["loans"]:
            loan = self._loans.get(str(loan_id))
            if loan is None or loan.status in LOAN_TERMINAL:
                continue
            self._kind_state(loan.lender, loan.kind).lent -= loan.open_amount
            self._kind_state(loan.borrower, loan.kind).borrowed -= loan.open_amount
            loan.open_amount = 0
            loan.status = LOAN_REFUNDED

    def _apply_settle(self, payload: dict) -> None:
        loan = self._loans[str(payload["loan_id"])]
        borrower = self._kind_state(loan.borrower, loan.kind)
        lender = self._kind_state(loan.lender, loan.kind)
        repaid = int(payload.get("repaid", 0))
        borrower.debt_hold += repaid
        borrower.borrowed -= repaid
        lender.lent -= repaid
        loan.open_amount -= repaid
        degrade = payload.get("degrade")
        if isinstance(degrade, dict):
            shortfall = int(degrade.get("shortfall", 0))
            borrower.borrowed -= shortfall
            lender.lent -= shortfall
            borrower.debt_hold = borrower.limit
            loan.open_amount -= shortfall
            loan.status = LOAN_DEGRADED
            self._frozen.add(loan.borrower)
            self._degraded.update(str(item) for item in degrade.get("cuts", []))
        else:
            loan.status = LOAN_REPAID

    def _apply_rollover(self, record: dict) -> None:
        self._period = int(record["to"])
        for kinds in self._scopes.values():
            for state in kinds.values():
                state.used = 0
                state.debt_hold = 0
        self._consumers = {}
        self._degraded = set()
        self._frozen = set()
        for loan_id in record.get("due", []):
            loan = self._loans.get(str(loan_id))
            if loan is not None and loan.status == LOAN_OPEN:
                loan.status = LOAN_DUE
        for payload in record.get("settles", []):
            if isinstance(payload, dict):
                self._apply_settle(payload)
