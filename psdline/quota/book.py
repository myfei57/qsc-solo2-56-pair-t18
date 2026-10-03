"""分级配额服务：在账本上执行借还、到发占用、周期结算与降级。

所有命令都走 :meth:`QuotaBook._run` 这同一个事务协议：

1. 进入即持有跨进程排他文件锁，折叠出当前台账；
2. 把台账交给命令的 ``handler`` 做全部业务校验、构造本事务事件；handler 抛
   :class:`QuotaError` 则**一个事件都不落盘**（部分失败整体回滚）；
3. handler 返回事件后，用同一把锁、同一个链头把整笔事务一次写入（原子提交）。

"读→校验→写"全程在锁内，多线程/多进程并发借还不会基于过期状态超卖。

取舍说明（两种模式都保留，风险在审计里可见）：

* 归还模式 ``auto``：释放占用即自动结清、周期窗口自动归还，无需人工介入；
  风险是窗口结束时仍在站的借款列车会被强制降级，需要用周期长度把误伤窗口管住。
* 归还模式 ``manual``：释放后贷款挂 ``due``，必须 :meth:`QuotaBook.confirm_return`
  人工确认才归还；风险是确认拖延会让额度沉淀、窗口结束触发降级。
* 降级策略默认 ``priority``：低优先级先砍，同级按到站序号（FIFO）；
  ``arrival`` 则完全按到站顺序砍，公平但不保护重点车。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Callable

from ..clock import Clock, SystemClock
from .ledger import LedgerEvent, LedgerTransaction, QuotaLedger
from .model import (
    ARRIVAL,
    DEPART,
    DOWNGRADE_POLICIES,
    KIND_NAMES,
    LEVEL_LINE,
    LEVEL_PLATFORM,
    LEVEL_TRAIN,
    LOAN_CLOSED,
    LOAN_DEFAULTED,
    LOAN_DUE,
    LOAN_OPEN,
    POLICY_ARRIVAL,
    POLICY_PRIORITY,
    QuotaError,
    QuotaState,
    RETURN_AUTO,
    RETURN_MANUAL,
    RETURN_MODES,
    active_total,
    fold,
    line_headroom_for,
    platform_handled,
)

Handler = Callable[[QuotaState], "list[LedgerEvent] | None"]


@dataclass(frozen=True)
class Grant:
    """初始化/调额/周期刷新时对一个实体的发放。"""

    entity: str
    level: str
    arrival: int
    depart: int
    parent: str = ""
    priority: int = 100

    def payload(self) -> dict:
        return {
            "entity": self.entity,
            "level": self.level,
            "parent": self.parent,
            "priority": self.priority,
            ARRIVAL: self.arrival,
            DEPART: self.depart,
        }


class QuotaBook:
    """分级配额台账服务。线程安全；多进程共用同一账本时由文件锁串行化。"""

    def __init__(
        self,
        ledger: QuotaLedger,
        clock: Clock | None = None,
        *,
        actor: str = "quota",
    ) -> None:
        self._ledger = ledger
        self._clock = clock or SystemClock()
        self._actor = actor
        self._base: list[LedgerTransaction] = []
        self._state: QuotaState = fold(ledger.read())

    # ---- 事务原语 -----------------------------------------------------

    @property
    def state(self) -> QuotaState:
        with self._ledger.transaction() as txn:
            return fold(txn.transactions)

    @property
    def ledger(self) -> QuotaLedger:
        return self._ledger

    def _run(self, idem: str, handler: Handler) -> QuotaState:
        """在持锁临界区内执行 ``handler(state)`` 并原子提交其返回的事件。

        * handler 抛异常 → 整笔不落盘（回滚）；
        * 返回 ``None`` → 只读，不提交；
        * 返回事件列表 → 一次追加为一笔事务；
        * ``idem`` 命中历史事务 → handler 不执行，直接回放（幂等重试）。
        """

        with self._ledger.transaction() as txn:
            state = fold(txn.transactions)
            if txn.find(idem) is not None:
                self._state = state
                return state
            self._base = list(txn.transactions)
            events = handler(state)
            if events is None:
                self._state = state
                return state
            if not events:
                raise QuotaError("空事务被拒绝")
            txn.append(
                events,
                txn_id=idem,
                at=self._clock.now(),
                actor=self._actor,
                idempotency_key=idem,
            )
            new_state = fold(txn.transactions)
            self._state = new_state
            return new_state

    def _stage(self, events: list[LedgerEvent]) -> QuotaState:
        """在内存把待定事件叠到 :attr:`_base` 上预演，不落盘、不重复取锁。"""

        head = self._base[-1].seq if self._base else 0
        staged = LedgerTransaction(
            seq=head + 1,
            txn_id="__stage__",
            at=self._clock.now(),
            actor=self._actor,
            events=tuple(events),
            prev_hash="",
            hash="",
        )
        return fold([*self._base, staged])

    @staticmethod
    def _idem(tag: str, key: str = "") -> str:
        return f"quota-{tag}-{key or uuid.uuid4().hex}"

    # ---- 装配与调额 ---------------------------------------------------

    def initialize(
        self,
        grants: list[Grant],
        *,
        return_mode: str = RETURN_AUTO,
        downgrade_policy: str = POLICY_PRIORITY,
        idem: str = "",
    ) -> QuotaState:
        """首次建账（幂等：已建账则原样返回）。"""

        def handle(state: QuotaState):
            if state.entities:
                return None
            self._validate_policy(return_mode, downgrade_policy)
            self._validate_grants(grants)
            return [
                LedgerEvent(
                    "initialized",
                    {
                        "return_mode": return_mode,
                        "downgrade_policy": downgrade_policy,
                        "grants": [grant.payload() for grant in grants],
                    },
                )
            ]

        return self._run(idem or self._idem("init"), handle)

    @staticmethod
    def _validate_policy(return_mode: str, downgrade_policy: str) -> None:
        if return_mode not in RETURN_MODES:
            raise QuotaError(f"未知归还模式: {return_mode}")
        if downgrade_policy not in DOWNGRADE_POLICIES:
            raise QuotaError(f"未知降级策略: {downgrade_policy}")

    @staticmethod
    def _validate_grants(grants: list[Grant]) -> None:
        lines = [grant for grant in grants if grant.level == LEVEL_LINE]
        if len(lines) != 1:
            raise QuotaError("线路额度必须恰好一份")
        line_id = lines[0].entity
        for grant in grants:
            if grant.arrival < 0 or grant.depart < 0:
                raise QuotaError(f"{grant.entity} 额度不能为负")
        for kind in KIND_NAMES:
            total_platform = sum(
                getattr(grant, kind)
                for grant in grants
                if grant.level == LEVEL_PLATFORM
            )
            if total_platform > getattr(lines[0], kind):
                raise QuotaError("站台发放之和超过线路总额度")
        for grant in grants:
            if grant.level == LEVEL_PLATFORM and grant.parent != line_id:
                raise QuotaError(f"站台 {grant.entity} 未挂在线路 {line_id} 下")
            if grant.level == LEVEL_TRAIN:
                parent = next(
                    (item for item in grants if item.entity == grant.parent), None
                )
                if parent is None or parent.level != LEVEL_PLATFORM:
                    raise QuotaError(f"列车 {grant.entity} 必须挂在某个站台下")

    def set_grants(self, grants: list[Grant], *, idem: str = "") -> QuotaState:
        """调整额度（只追加 grants_set；历史发放保留在账上）。"""

        def handle(state: QuotaState):
            if not state.entities:
                raise QuotaError("账本尚未初始化")
            adjusted = {grant.entity: grant for grant in grants}
            line_id = _entity_id(state, LEVEL_LINE)

            def grant_of(entity_id: str, kind: str) -> int:
                if entity_id in adjusted:
                    return int(getattr(adjusted[entity_id], kind))
                return state.entity(entity_id).account(kind).granted

            for grant in grants:
                if grant.entity not in state.entities:
                    raise QuotaError(f"实体未登记，不能直接调额: {grant.entity}")
                if grant.arrival < 0 or grant.depart < 0:
                    raise QuotaError(f"{grant.entity} 额度不能为负")
            platforms = [e for e in state.entities.values() if e.level == LEVEL_PLATFORM]
            for kind in KIND_NAMES:
                if sum(grant_of(p.id, kind) for p in platforms) > grant_of(line_id, kind):
                    raise QuotaError("调额后站台发放之和超过线路总量")
            for entity in state.entities.values():
                for kind in KIND_NAMES:
                    if entity.account(kind).held > grant_of(entity.id, kind):
                        raise QuotaError(
                            f"调额后 {entity.id} 的 {kind} 发放量低于在站占用，先腾位再调"
                        )
            return [
                LedgerEvent("grants_set", {"grants": [g.payload() for g in grants]})
            ]

        return self._run(idem or self._idem("grants"), handle)

    def register_train(
        self,
        train_id: str,
        platform_id: str,
        arrival: int,
        depart: int,
        *,
        priority: int = 100,
        idem: str = "",
    ) -> QuotaState:
        """新列车建档并发放本周期额度（幂等）。"""

        def handle(state: QuotaState):
            if train_id in state.entities:
                return None
            platform = state.entities.get(platform_id)
            if platform is None or platform.level != LEVEL_PLATFORM:
                raise QuotaError(f"站台未登记: {platform_id}")
            if arrival < 0 or depart < 0:
                raise QuotaError("列车额度不能为负")
            grant = Grant(
                entity=train_id, level=LEVEL_TRAIN, arrival=arrival, depart=depart,
                parent=platform_id, priority=priority,
            )
            return [LedgerEvent("grants_set", {"grants": [grant.payload()]})]

        return self._run(idem or self._idem("train", train_id), handle)

    def set_priority(
        self, train_id: str, priority: int, *, idem: str = ""
    ) -> QuotaState:
        def handle(state: QuotaState):
            train = state.entities.get(train_id)
            if train is None or train.level != LEVEL_TRAIN:
                raise QuotaError(f"列车未登记: {train_id}")
            return [
                LedgerEvent(
                    "priority_set", {"train": train_id, "priority": int(priority)}
                )
            ]

        return self._run(
            idem or self._idem("prio", f"{train_id}-{priority}"), handle
        )

    def set_policy(
        self,
        *,
        return_mode: str | None = None,
        downgrade_policy: str | None = None,
        idem: str = "",
    ) -> QuotaState:
        def handle(state: QuotaState):
            mode = state.return_mode if return_mode is None else return_mode
            policy = (
                state.downgrade_policy if downgrade_policy is None else downgrade_policy
            )
            self._validate_policy(mode, policy)
            payload: dict = {}
            if return_mode is not None:
                payload["return_mode"] = return_mode
            if downgrade_policy is not None:
                payload["downgrade_policy"] = downgrade_policy
            if not payload:
                return None
            return [LedgerEvent("policy_set", payload)]

        return self._run(
            idem or self._idem("policy", f"{return_mode or ''}-{downgrade_policy or ''}"),
            handle,
        )

    # ---- 到发占用与借还 ----------------------------------------------

    def reserve(
        self, kind: str, train_id: str, platform_id: str, *, idem: str = ""
    ) -> QuotaState:
        """批准一次到站/发车；不够时沿 列车→站台→线路 向上借。

        每个单位要么用 ``"own"``（列车自有），要么开一笔单位贷款（可能两级链路）。
        三级资金在本事务内一次校验、一次落账，任一级不足整笔拒绝。
        """

        def handle(state: QuotaState):
            if kind not in KIND_NAMES:
                raise QuotaError(f"未知额度类型: {kind}")
            train, platform, line = state.chain_of(train_id)
            if platform.id != platform_id:
                raise QuotaError(
                    f"列车 {train_id} 挂靠 {platform.id}，不能占用 {platform_id}"
                )
            if train.degraded:
                raise QuotaError(f"列车 {train_id} 已降级，停止受理到发")
            overdue = [
                loan
                for loan in state.open_loans(status=LOAN_DUE)
                if loan.borrower == train_id and loan.cycle < state.cycle
            ]
            if overdue:
                raise QuotaError(
                    f"列车 {train_id} 有逾期未确认归还的借款 {sorted(l.id for l in overdue)}，"
                    "先人工确认归还再受理"
                )
            if kind == ARRIVAL and state.active_arrival(train_id) is not None:
                raise QuotaError(f"列车 {train_id} 已有在站占用，不能重复到站")

            # 第一道闸门是物理的：线路总车位（granted）是全线在站/发车占用的硬
            # 上限，与这笔钱最终由列车、站台还是线路出资无关。满了一律拒绝，即便
            # 某站台自己的切片还没用过。
            if active_total(state, kind) >= line.account(kind).granted:
                raise QuotaError(
                    f"{kind} 额度不足：线路总车位 {line.account(kind).granted} 已满，"
                    f"列车 {train_id} 被拒绝"
                )

            loan_seq = state.loan_seq
            if train.account(kind).available >= 1:
                sources = ["own"]
                loan_events: list[LedgerEvent] = []
            else:
                loan_seq, loan_events, sources = self._borrow_chain_events(
                    state, loan_seq, kind, train, platform, line
                )

            permit_seq = state.permit_seq + 1
            permit_id = f"P{permit_seq:08d}"
            for event in loan_events:
                event.payload["permit"] = permit_id
            permit_payload = {
                "id": permit_id,
                "kind": kind,
                "cycle": state.cycle,
                "train": train_id,
                "platform": platform_id,
                "at": self._clock.now(),
                "sources": sources,
                "seq": permit_seq,
            }
            if kind == ARRIVAL:
                permit_payload["arrival_seq"] = _max_arrival_seq(state) + 1
            return [*loan_events, LedgerEvent("permit_issued", permit_payload)]

        key = idem or self._idem("reserve", f"{kind}-{train_id}")
        return self._run(key, handle)

    @staticmethod
    def _borrow_chain_events(
        state: QuotaState,
        seq: int,
        kind: str,
        train,
        platform,
        line,
    ) -> tuple[int, list[LedgerEvent], list[str]]:
        """构造 列车→站台（必要时再 →线路） 的单位贷款事件与资金来源。

        两级借钱资格独立判断：

        * 站台自己有空闲（``granted - held``，held 是资金溯源口径）→ 只开列车贷款；
        * 站台空、但**线路对该站台还有空余额度**（线路发放减去该站台已动用的
          溯源占用）→ 站台中转，开两级链路贷款；
        * 线路也空 → 整笔拒绝，绝不超卖。
        """

        def loan_event(loan_id, borrower, borrower_level, lender, lender_level, chain=""):
            return LedgerEvent(
                "loan_opened",
                {
                    "id": loan_id,
                    "seq": int(loan_id[1:]),
                    "kind": kind,
                    "cycle": state.cycle,
                    "borrower": borrower,
                    "borrower_level": borrower_level,
                    "lender": lender,
                    "lender_level": lender_level,
                    "permit": "",
                    "mode": state.return_mode,
                    "chain": chain,
                },
            )

        handled = platform_handled(state, kind, platform.id)
        if handled < platform.account(kind).granted:
            # 站台自己这份切片还没经手满：钱由站台自有出资，线路不动。
            # （注意要用"经手总量"而不是溯源 held：经手中转的车虽最终溯源到
            # 线路，但同样占用该站台切片名额。）
            seq += 1
            train_loan = f"L{seq:08d}"
            event = loan_event(
                train_loan, train.id, LEVEL_TRAIN, platform.id, LEVEL_PLATFORM
            )
            return seq, [event], [train_loan]

        # 站台切片已经手满，由站台作通道中转线路的钱。唯一硬约束是线路总车位：
        # 全线活动占用合计不得超过线路发放量。线路也满 → 整笔拒绝，绝不超卖。
        if line_headroom_for(state, kind, platform.id, line.id) < 1:
            raise QuotaError(
                f"{kind} 额度不足：站台 {platform.id} 切片已满且线路总车位用尽，"
                f"列车 {train.id} 被拒绝"
            )
            raise QuotaError(
                f"{kind} 额度不足：线路对站台 {platform.id} 与该站台均无空闲，"
                f"列车 {train.id} 被拒绝"
            )
        seq += 1
        line_loan = f"L{seq:08d}"
        seq += 1
        train_loan = f"L{seq:08d}"
        events = [
            loan_event(line_loan, platform.id, LEVEL_PLATFORM, line.id, LEVEL_LINE),
            loan_event(
                train_loan, train.id, LEVEL_TRAIN, platform.id, LEVEL_PLATFORM,
                chain=line_loan,
            ),
        ]
        return seq, events, [train_loan]

    def release(
        self, permit_id: str, *, reason: str = "", idem: str = ""
    ) -> QuotaState:
        """释放占用；关联贷款 auto→closed，manual→due 挂账。"""

        def handle(state: QuotaState):
            permit = state.permits.get(permit_id)
            if permit is None:
                raise QuotaError(f"占用不存在: {permit_id}")
            if not permit.active:
                return None
            return self._release_events(state, permit, reason or "released")

        return self._run(idem or self._idem("release", permit_id), handle)

    def _release_events(self, state: QuotaState, permit, reason: str) -> list[LedgerEvent]:
        """构造"释放占用 + 关联贷款结清/挂账"的事件列表。"""

        events: list[LedgerEvent] = [
            LedgerEvent(
                "permit_released",
                {"id": permit.id, "at": self._clock.now(), "reason": reason},
            )
        ]
        for source in permit.sources:
            if source == "own":
                continue
            loan = state.loans[source]
            if loan.status != LOAN_OPEN:
                continue
            events.append(
                LedgerEvent(
                    "loan_status_changed",
                    {
                        "id": loan.id,
                        "status": LOAN_CLOSED if loan.mode == RETURN_AUTO else LOAN_DUE,
                    },
                )
            )
            if loan.chain:
                upstream = state.loans[loan.chain]
                if upstream.status == LOAN_OPEN:
                    events.append(
                        LedgerEvent(
                            "loan_status_changed",
                            {
                                "id": upstream.id,
                                "status": (
                                    LOAN_CLOSED
                                    if upstream.mode == RETURN_AUTO
                                    else LOAN_DUE
                                ),
                            },
                        )
                    )
        return events

    def confirm_return(
        self, loan_ids: list[str] | None = None, *, idem: str = ""
    ) -> dict:
        """人工确认归还（manual 模式）。只认 ``due`` 贷款；逐笔独立成账。

        各笔贷款互不影响：前面确认成功的不会因后面失败而回滚（每笔一笔事务），
        但调用方拿到的是逐笔结果，失败原因可见。
        """

        with self._ledger.transaction() as txn:
            state = fold(txn.transactions)
            targets = list(state.open_loans(status=LOAN_DUE))
            if loan_ids is not None:
                wanted = set(loan_ids)
                targets = [loan for loan in targets if loan.id in wanted]
                missing = wanted - {loan.id for loan in targets}
                if missing:
                    raise QuotaError(f"待归还贷款不存在或状态不对: {sorted(missing)}")
            rows = []
            for loan in targets:
                key = idem or self._idem("confirm", loan.id)
                if txn.find(key) is not None:
                    rows.append({"loan": loan.id, "returned": True})
                    continue
                events = [
                    LedgerEvent(
                        "loan_status_changed", {"id": loan.id, "status": LOAN_CLOSED}
                    )
                ]
                if loan.chain:
                    upstream = state.loans[loan.chain]
                    if upstream.status == LOAN_DUE:
                        events.append(
                            LedgerEvent(
                                "loan_status_changed",
                                {"id": upstream.id, "status": LOAN_CLOSED},
                            )
                        )
                txn.append(
                    events,
                    txn_id=key,
                    at=self._clock.now(),
                    actor=self._actor,
                    idempotency_key=key,
                )
                state = fold(txn.transactions)
                rows.append({"loan": loan.id, "returned": True})
            self._state = state
            return {"ok": True, "returned": rows, "count": len(rows)}

    # ---- 周期结算与降级 ----------------------------------------------

    def settle_cycle(
        self, *, new_grants: list[Grant] | None = None, idem: str = ""
    ) -> dict:
        """周期切换时间窗（**单笔原子事务**）。

        新周期顺序：刷新各级额度 → 窗口内归还 due 贷款（出资方新额度装得下才
        还）→ 跨周期仍占用借款的列车按策略降级、释放到站占用 → 仍未结清的贷款
        标 ``defaulted`` 挂账（历史不可变，后续可人工补还）。全部事件先在内存
        候选台账上预演，任何一步失败整笔不落账。幂等：重复结算同一周期 noop。
        """

        with self._ledger.transaction() as txn:
            base = list(txn.transactions)
            self._base = base
            state = fold(base)
            next_cycle = state.cycle + 1
            # 默认每次结算推进一个周期；仅当调用方显式给出 idem 键且该键已存在时
            # 才去重（防重试重复结算）。
            key = idem or self._idem("settle", str(next_cycle))
            if idem and txn.find(key) is not None:
                return {"ok": True, "noop": True, "cycle": state.cycle}

            grants = (
                new_grants if new_grants is not None else self._current_grants(state)
            )
            self._validate_refresh(state, grants)

            events: list[LedgerEvent] = [
                LedgerEvent(
                    "cycle_advanced",
                    {
                        "cycle": next_cycle,
                        "grants": [g.payload() for g in grants],
                        "settlement": next_cycle,
                    },
                )
            ]

            # 1) due 贷款（上周期车已走、等归还的）在新额度内尽量归还。
            returned: list[str] = []
            candidate = self._stage(events)
            due = sorted(
                candidate.open_loans(status=LOAN_DUE),
                key=lambda loan: (0 if loan.chain else 1, loan.id),
            )
            for loan in due:
                if candidate.entity(loan.lender).account(loan.kind).available >= 1:
                    events.append(
                        LedgerEvent(
                            "loan_status_changed",
                            {"id": loan.id, "status": LOAN_CLOSED},
                        )
                    )
                    candidate = self._stage(events)
                    returned.append(loan.id)

            # 2) 上周期借出、此刻仍在站（钱还不上）的列车。
            overdue_trains = sorted(
                {
                    loan.borrower
                    for loan in candidate.open_loans(status=LOAN_OPEN)
                    if loan.cycle < next_cycle
                    and loan.borrower_level == LEVEL_TRAIN
                }
            )
            downgraded: list[dict] = []
            overdue_marked: list[str] = []
            if candidate.return_mode == RETURN_AUTO:
                # 时间窗自动归还：auto 模式直接按策略砍车腾位、结清贷款。风险是
                # 误伤仍在站的车，靠周期长度把误伤窗口管住。
                ordered = self._order_by_policy(candidate, overdue_trains)
                for train_id in ordered:
                    row = self._downgrade_events(
                        candidate,
                        events,
                        train_id,
                        reason=f"周期 {next_cycle} 窗口结束仍未归还借款（auto 自动降级）",
                    )
                    if row is not None:
                        candidate = self._stage(events)
                        downgraded.append(row)
            else:
                # 人工确认归还：不自动砍车。到期 open 贷款转 due 挂账等确认，
                # 列车标 overdue（reserve 会拒绝其新到发）。风险是人工拖延导致
                # 额度沉淀；钉子户由 downgrade_for_capacity / 人工 downgrade 处理。
                for train_id in overdue_trains:
                    permit = candidate.active_arrival(train_id)
                    if permit is None:
                        continue
                    for source in permit.sources:
                        if source == "own":
                            continue
                        loan = candidate.loans[source]
                        if loan.status == LOAN_OPEN and loan.cycle < next_cycle:
                            events.append(
                                LedgerEvent(
                                    "loan_status_changed",
                                    {"id": loan.id, "status": LOAN_DUE},
                                )
                            )
                            overdue_marked.append(loan.id)
                            if loan.chain:
                                upstream = candidate.loans[loan.chain]
                                if upstream.status == LOAN_OPEN:
                                    events.append(
                                        LedgerEvent(
                                            "loan_status_changed",
                                            {"id": upstream.id, "status": LOAN_DUE},
                                        )
                                    )
                            candidate = self._stage(events)

            # 3) 兜底：窗口结束仍是 due/open 的历史贷款标 defaulted 永久挂账。
            #    auto 模式下降级已释放占用、贷款已 closed，一般不会走到这里；
            #    manual 模式下 due 保留等人工，不在这里打 defaulted。
            defaulted: list[str] = []
            if candidate.return_mode == RETURN_AUTO:
                for loan in candidate.open_loans():
                    if loan.status == LOAN_OPEN and loan.cycle < next_cycle:
                        events.append(
                            LedgerEvent(
                                "loan_status_changed",
                                {"id": loan.id, "status": LOAN_DEFAULTED},
                            )
                        )
                        defaulted.append(loan.id)
                        candidate = self._stage(events)

            txn.append(
                events,
                txn_id=key,
                at=self._clock.now(),
                actor=self._actor,
                idempotency_key=key,
            )
            self._state = fold(txn.transactions)
            return {
                "ok": True,
                "cycle": next_cycle,
                "return_mode": candidate.return_mode,
                "returned": returned,
                "defaulted": sorted(set(defaulted)),
                "downgraded": downgraded,
                "overdue_loans": sorted(set(overdue_marked)),
            }

    @staticmethod
    def _validate_refresh(state: QuotaState, grants: list[Grant]) -> None:
        by_id = {grant.entity: grant for grant in grants}
        if set(by_id) != {entity.id for entity in state.entities.values()}:
            raise QuotaError("周期刷新必须覆盖全部已登记实体")
        line = next(e for e in state.entities.values() if e.level == LEVEL_LINE)
        for kind in KIND_NAMES:
            total_platform = sum(
                getattr(by_id[entity.id], kind)
                for entity in state.entities.values()
                if entity.level == LEVEL_PLATFORM
            )
            if total_platform > getattr(by_id[line.id], kind):
                raise QuotaError("刷新后站台发放之和超过线路总量")

    @staticmethod
    def _order_by_policy(state: QuotaState, train_ids: list[str]) -> list[str]:
        trains = [state.entity(train_id) for train_id in train_ids]
        if state.downgrade_policy == POLICY_ARRIVAL:
            trains.sort(key=lambda entity: entity.arrival_seq)
        else:
            trains.sort(key=lambda entity: (-entity.priority, entity.arrival_seq))
        return [entity.id for entity in trains]

    def downgrade(
        self, train_id: str, *, reason: str = "manual", idem: str = ""
    ) -> dict:
        """人工/应急降级一列车。"""

        result: dict = {}

        def handle(state: QuotaState):
            train = state.entities.get(train_id)
            if train is None or train.level != LEVEL_TRAIN:
                raise QuotaError(f"列车未登记: {train_id}")
            events: list[LedgerEvent] = []
            row = self._downgrade_events(state, events, train_id, reason)
            if row is None:
                raise QuotaError(f"列车无需降级: {train_id}")
            result.update(row)
            return events

        self._run(idem or self._idem("downgrade", train_id), handle)
        return result

    def _downgrade_events(
        self,
        candidate: QuotaState,
        events: list[LedgerEvent],
        train_id: str,
        reason: str,
    ) -> dict | None:
        """把一列车的降级事件追加进 ``events``（预演/正式共用）。"""

        train = candidate.entities.get(train_id)
        if train is None or train.level != LEVEL_TRAIN:
            return None
        permit = candidate.active_arrival(train_id)
        loan_ids: list[str] = []
        if permit is not None:
            events.append(
                LedgerEvent(
                    "permit_released",
                    {"id": permit.id, "at": self._clock.now(), "reason": "downgrade"},
                )
            )
            for source in permit.sources:
                if source == "own":
                    continue
                loan = candidate.loans[source]
                loan_ids.append(loan.id)
                events.append(
                    LedgerEvent(
                        "loan_status_changed",
                        {
                            "id": loan.id,
                            "status": (
                                LOAN_CLOSED if loan.mode == RETURN_AUTO else LOAN_DUE
                            ),
                        },
                    )
                )
                if loan.chain:
                    upstream = candidate.loans[loan.chain]
                    events.append(
                        LedgerEvent(
                            "loan_status_changed",
                            {
                                "id": upstream.id,
                                "status": (
                                    LOAN_CLOSED
                                    if upstream.mode == RETURN_AUTO
                                    else LOAN_DUE
                                ),
                            },
                        )
                    )
        if not train.degraded:
            events.append(
                LedgerEvent(
                    "train_downgraded",
                    {
                        "train": train_id,
                        "reason": reason,
                        "seq": candidate.downgraded_seq + 1,
                    },
                )
            )
        return {
            "train": train_id,
            "permit_released": permit.id if permit else "",
            "loans": loan_ids,
            "reason": reason,
        }

    def downgrade_for_capacity(
        self, kind: str, platform_id: str, *, need: int = 1, idem: str = ""
    ) -> list[dict]:
        """额度不足时按当前策略砍车腾位，返回降级清单（单笔事务）。"""

        def handle(state: QuotaState):
            candidates = [
                entity
                for entity in state.entities.values()
                if entity.level == LEVEL_TRAIN
                and entity.parent == platform_id
                and not entity.degraded
                and state.active_arrival(entity.id) is not None
            ]
            if state.downgrade_policy == POLICY_ARRIVAL:
                candidates.sort(key=lambda entity: entity.arrival_seq)
            else:
                candidates.sort(
                    key=lambda entity: (-entity.priority, entity.arrival_seq)
                )
            events: list[LedgerEvent] = []
            outcomes: list[dict] = []
            staged = state
            for entity in candidates[:need]:
                row = self._downgrade_events(
                    staged,
                    events,
                    entity.id,
                    reason=f"为 {kind} 腾位（{state.downgrade_policy} 策略）",
                )
                if row and row["permit_released"]:
                    staged = self._stage(events)
                    outcomes.append(row)
            if not outcomes:
                return None
            result_rows.extend(outcomes)
            return events

        result_rows: list[dict] = []
        self._run(idem or self._idem("reclaim", f"{kind}-{platform_id}-{need}"), handle)
        return result_rows

    def restore(self, train_id: str, *, idem: str = "") -> QuotaState:
        """恢复被降级的列车，重新受理到发。"""

        def handle(state: QuotaState):
            train = state.entities.get(train_id)
            if train is None or train.level != LEVEL_TRAIN:
                raise QuotaError(f"列车未登记: {train_id}")
            if not train.degraded:
                return None
            return [
                LedgerEvent(
                    "train_restored", {"train": train_id, "cycle": state.cycle}
                )
            ]

        return self._run(idem or self._idem("restore", train_id), handle)

    def repay_defaulted(self, loan_id: str, *, idem: str = "") -> QuotaState:
        """人工补还历史挂账贷款（defaulted → closed），只追加、不改历史。"""

        def handle(state: QuotaState):
            loan = state.loans.get(loan_id)
            if loan is None:
                raise QuotaError(f"贷款不存在: {loan_id}")
            if loan.status != LOAN_DEFAULTED:
                raise QuotaError(f"贷款不是挂账状态: {loan_id}（{loan.status}）")
            events = [
                LedgerEvent(
                    "loan_status_changed", {"id": loan_id, "status": LOAN_CLOSED}
                )
            ]
            if loan.chain:
                upstream = state.loans[loan.chain]
                if upstream.status == LOAN_DEFAULTED:
                    events.append(
                        LedgerEvent(
                            "loan_status_changed",
                            {"id": upstream.id, "status": LOAN_CLOSED},
                        )
                    )
            return events

        return self._run(idem or self._idem("repay", loan_id), handle)

    # ---- 查询 ---------------------------------------------------------

    @staticmethod
    def _current_grants(state: QuotaState) -> list[Grant]:
        return [
            Grant(
                entity=entity.id,
                level=entity.level,
                arrival=entity.account(ARRIVAL).granted,
                depart=entity.account(DEPART).granted,
                parent=entity.parent,
                priority=entity.priority,
            )
            for entity in state.entities.values()
        ]

    def status(self) -> dict:
        return self.state.status()

    def audit_events(self, limit: int = 200) -> list[dict]:
        """把账本事务翻译成审计条目形态，供与既有审计流合并。"""

        with self._ledger.transaction() as txn:
            transactions = list(txn.transactions)
        return [
            {
                "id": f"quota-{txn.seq:08d}",
                "kind": "quota_txn",
                "train_id": "",
                "station_id": "",
                "at": txn.at,
                "detail": {
                    "txn_id": txn.txn_id,
                    "actor": txn.actor,
                    "events": [
                        {"type": event.type, **event.payload}
                        for event in txn.events
                    ],
                },
            }
            for txn in transactions[-limit:]
        ]


def _entity_id(state: QuotaState, level: str) -> str:
    for entity in state.entities.values():
        if entity.level == level:
            return entity.id
    raise QuotaError(f"账上没有 {level} 级实体")


def _max_arrival_seq(state: QuotaState) -> int:
    return max(
        (
            entity.arrival_seq
            for entity in state.entities.values()
            if entity.level == LEVEL_TRAIN
        ),
        default=0,
    )
