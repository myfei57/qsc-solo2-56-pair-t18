"""分级配额的领域模型与纯函数投影。

账本事件经过 :func:`fold` 折叠成 :class:`QuotaState`。投影是**纯函数**：同样的
事件序列必然得到同样的状态，因此重启、回放、审计核对走的是同一条折叠路径，
不存在"账本一套、内存一套"。

记账口径（关键约定，避免重复计数）：

* 每个实体（线路/站台/列车）对到站、发车各有一份 ``granted``（本周期发放）。
* 一个实际占用（hold）只记在**出资方**名下：
    - 列车用自己额度到站 → 记 ``train.hold``；
    - 列车额度不够、站台借出 → 记 ``platform.hold``，同时列车背上一笔贷款；
    - 站台也不够、线路借出 → 记 ``line.hold``，站台背上贷款（可能同时转贷给
      列车，形成链路 ``line → platform → train``）。
* 因此 ``线路.hold`` 恒等于全线路实际占用数，线路发放量是硬上限；站台之间不
  会互相挤占，苦乐不均由"向上借"而不是"平调"解决。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from ..errors import RailError
from .ledger import LedgerTransaction

ARRIVAL = "arrival"
DEPART = "depart"
KIND_NAMES = (ARRIVAL, DEPART)

LEVEL_LINE = "line"
LEVEL_PLATFORM = "platform"
LEVEL_TRAIN = "train"
LEVELS = (LEVEL_LINE, LEVEL_PLATFORM, LEVEL_TRAIN)

RETURN_AUTO = "auto"
RETURN_MANUAL = "manual"
RETURN_MODES = (RETURN_AUTO, RETURN_MANUAL)

POLICY_PRIORITY = "priority"
POLICY_ARRIVAL = "arrival"
DOWNGRADE_POLICIES = (POLICY_PRIORITY, POLICY_ARRIVAL)

LOAN_OPEN = "open"        # 槽位仍在占用，贷款未到期
LOAN_DUE = "due"          # 槽位已释放，待归还（manual 模式等确认）
LOAN_CLOSED = "closed"    # 已归还
LOAN_DEFAULTED = "defaulted"  # 周期窗口结束仍未还上，已触发降级/挂账


class QuotaError(RailError):
    """配额条件不满足（额度不足、状态非法等），整笔事务被拒绝。"""

    def __init__(self, message: str) -> None:
        super().__init__(message, component="quota")


@dataclass(frozen=True)
class Loan:
    """一笔向上级借用的额度，单位为 1，可逐笔归还。"""

    id: str
    kind: str
    cycle: int
    borrower: str
    borrower_level: str
    lender: str
    lender_level: str
    permit_id: str          # 这笔钱支撑的占用标识
    mode: str
    status: str = LOAN_OPEN
    chain: str = ""         # 链路下一级贷款（站台转贷给列车时）

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "cycle": self.cycle,
            "borrower": self.borrower,
            "borrower_level": self.borrower_level,
            "lender": self.lender,
            "lender_level": self.lender_level,
            "permit_id": self.permit_id,
            "mode": self.mode,
            "status": self.status,
            "chain": self.chain,
        }


@dataclass
class KindAccount:
    """一个实体在一种额度（到站/发车）上的本周期账目。"""

    granted: int = 0
    held: int = 0           # 出资方口径的占用数
    used_total: int = 0     # 历史累计发放的占用数（只增，审计用）

    @property
    def available(self) -> int:
        """可再出资的数量：本周期发放减去已被占用。"""

        return max(0, self.granted - self.held)

    def as_dict(self) -> dict:
        return {
            "granted": self.granted,
            "held": self.held,
            "available": self.available,
            "used_total": self.used_total,
        }


@dataclass
class EntityState:
    """线路、站台或列车的配额状态。"""

    id: str
    level: str
    parent: str = ""
    priority: int = 100          # 数字越小优先级越高
    arrival_seq: int = 0         # 最近一次到站序号（降级排序用）
    degraded: bool = False
    downgrade_reason: str = ""
    restored_cycle: int = 0
    accounts: dict[str, KindAccount] = field(default_factory=dict)

    def account(self, kind: str) -> KindAccount:
        if kind not in self.accounts:
            self.accounts[kind] = KindAccount()
        return self.accounts[kind]

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "level": self.level,
            "parent": self.parent,
            "priority": self.priority,
            "degraded": self.degraded,
            "downgrade_reason": self.downgrade_reason,
            "arrival_seq": self.arrival_seq,
            "arrival": self.account(ARRIVAL).as_dict(),
            "depart": self.account(DEPART).as_dict(),
        }


@dataclass
class Permit:
    """一次到/发占用。

    到站 permit 从批准持续到发车；发车 permit 瞬时（批准即关闭），但仍保留记录
    供审计。``sources`` 列出本次占用每个单位的出资方：``"own"`` 或贷款 id。
    """

    id: str
    kind: str
    cycle: int
    train_id: str
    platform_id: str
    at: float
    sources: tuple[str, ...]
    active: bool = True
    released_at: float | None = None
    release_reason: str = ""

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "kind": self.kind,
            "cycle": self.cycle,
            "train_id": self.train_id,
            "platform_id": self.platform_id,
            "at": self.at,
            "sources": list(self.sources),
            "active": self.active,
            "released_at": self.released_at,
            "release_reason": self.release_reason,
        }


@dataclass
class QuotaState:
    """折叠后的完整台账。"""

    cycle: int = 0
    return_mode: str = RETURN_AUTO
    downgrade_policy: str = POLICY_PRIORITY
    entities: dict[str, EntityState] = field(default_factory=dict)
    permits: dict[str, Permit] = field(default_factory=dict)
    loans: dict[str, Loan] = field(default_factory=dict)
    permit_seq: int = 0
    loan_seq: int = 0
    downgraded_seq: int = 0
    last_settlement_cycle: int = 0
    ledger_head: int = 0

    # ---- 查询 ---------------------------------------------------------

    def entity(self, entity_id: str) -> EntityState:
        found = self.entities.get(entity_id)
        if found is None:
            raise QuotaError(f"配额实体未登记: {entity_id}")
        return found

    def chain_of(self, train_id: str) -> list[EntityState]:
        """返回 ``[列车, 站台, 线路]``。"""

        train = self.entity(train_id)
        platform = self.entity(train.parent)
        line = self.entity(platform.parent)
        return [train, platform, line]

    def active_arrival(self, train_id: str) -> Permit | None:
        """一列车至多有一个在站到站占用。"""

        for permit in self.permits.values():
            if (
                permit.kind == ARRIVAL
                and permit.train_id == train_id
                and permit.active
            ):
                return permit
        return None

    def open_loans(self, *, borrower: str = "", status: str = "") -> list[Loan]:
        loans = list(self.loans.values())
        if borrower:
            loans = [loan for loan in loans if loan.borrower == borrower]
        if status:
            loans = [loan for loan in loans if loan.status == status]
        return sorted(loans, key=lambda loan: loan.id)

    # ---- 序列化 -------------------------------------------------------

    def account_view(self, entity: EntityState, kind: str) -> dict:
        account = entity.account(kind)
        open_borrow = sum(
            1
            for loan in self.loans.values()
            if loan.kind == kind
            and loan.borrower == entity.id
            and loan.status in (LOAN_OPEN, LOAN_DUE)
        )
        open_lend = sum(
            1
            for loan in self.loans.values()
            if loan.kind == kind
            and loan.lender == entity.id
            and loan.status in (LOAN_OPEN, LOAN_DUE)
        )
        return {
            **account.as_dict(),
            "borrowing": open_borrow,
            "lending": open_lend,
        }

    def status(self) -> dict:
        """整份分级配额快照（控制台/CLI 读取同一份）。"""

        def view(entity: EntityState) -> dict:
            return {
                **entity.as_dict(),
                "arrival": self.account_view(entity, ARRIVAL),
                "depart": self.account_view(entity, DEPART),
            }

        line = next((item for item in self.entities.values() if item.level == LEVEL_LINE), None)
        return {
            "cycle": self.cycle,
            "return_mode": self.return_mode,
            "downgrade_policy": self.downgrade_policy,
            "last_settlement_cycle": self.last_settlement_cycle,
            "active_arrivals": sum(
                1
                for permit in self.permits.values()
                if permit.kind == ARRIVAL and permit.active
            ),
            "open_loans": len(self.open_loans(status=LOAN_OPEN)),
            "due_loans": len(self.open_loans(status=LOAN_DUE)),
            "defaulted_loans": len(self.open_loans(status=LOAN_DEFAULTED)),
            "degraded_trains": [
                entity.id for entity in self.entities.values()
                if entity.level == LEVEL_TRAIN and entity.degraded
            ],
            "ledger_head": self.ledger_head,
            "line": view(line) if line else None,
            "platforms": [
                view(item)
                for item in self.entities.values()
                if item.level == LEVEL_PLATFORM
            ],
            "trains": [
                view(item)
                for item in self.entities.values() if item.level == LEVEL_TRAIN
            ],
        }


# ---------------------------------------------------------------------------
# 事件折叠（纯函数）
# ---------------------------------------------------------------------------


def _ensure_entity(
    state: QuotaState,
    entity_id: str,
    level: str,
    parent: str = "",
    priority: int = 100,
) -> EntityState:
    if level not in LEVELS:
        raise QuotaError(f"未知配额层级: {level}")
    entity = state.entities.get(entity_id)
    if entity is None:
        entity = EntityState(id=entity_id, level=level, parent=parent, priority=priority)
        state.entities[entity_id] = entity
    elif parent and not entity.parent:
        entity.parent = parent
    return entity


def _set_grants(state: QuotaState, payload: dict) -> None:
    for raw in payload.get("grants", ()):  # type: dict
        entity = _ensure_entity(
            state,
            str(raw["entity"]),
            str(raw["level"]),
            str(raw.get("parent", "")),
            int(raw.get("priority", 100)),
        )
        for kind in KIND_NAMES:
            value = raw.get(kind)
            if value is not None:
                entity.account(kind).granted = int(value)


def _fund_holder(state: "QuotaState", loan_id: str) -> str:
    """沿 ``chain`` 溯源到真正出资的一级（链路贷款的最顶端）。

    例如 线路→站台→列车 的两级贷款，列车那笔贷款的直接 lender 是站台，但钱
    实际来自线路；占用必须记在线路名下，否则站台会被重复计数。
    """

    loan = state.loans[loan_id]
    while loan.chain:
        loan = state.loans[loan.chain]
    return loan.lender


def line_headroom_for(state: "QuotaState", kind: str, platform_id: str, line_id: str) -> int:
    """还能再放进全线多少（线路是全线硬总车位）。

    资金优先用列车自有、再用站台自有、最后才由线路池兜底；但无论钱出自哪一级，
    每一条活动占用都停在线路的物理区间里。因此线路发放量是**全线活动占用总数**
    的硬上限，而不只是"线路兜底口袋"的上限：

        可再放量 = 线路发放量 - 全线当前活动占用数（跨所有站台合计）。

    站台发放量是线路总量里的切片（各片之和 ≤ 线路总量），决定该站台自有能托底
    多少；中转线路池时不消耗切片，只受这个总车位约束。
    """

    line_granted = state.entity(line_id).account(kind).granted
    active_total = sum(
        len(permit.sources)
        for permit in state.permits.values()
        if permit.active and permit.kind == kind
    )
    return max(0, line_granted - active_total)


def active_total(state: QuotaState, kind: str) -> int:
    """全线某类额度的当前活动占用总数（跨站台合计）。"""

    return sum(
        len(permit.sources)
        for permit in state.permits.values()
        if permit.active and permit.kind == kind
    )


def platform_handled(state: "QuotaState", kind: str, platform_id: str) -> int:
    """某站台当前经手的在站占用单位总数（不论钱最终出自哪一级）。

    站台是线路额度的"切片"：经它放进来的车不能超过自己那份发放量，否则一个
    站台会把线路共享池虹吸光，其余站台失去线路托底。
    """

    total = 0
    for permit in state.permits.values():
        if not permit.active or permit.kind != kind or permit.platform_id != platform_id:
            continue
        total += len(permit.sources)
    return total


def _apply(state: QuotaState, event_type: str, payload: dict) -> None:
    if event_type == "initialized":
        state.return_mode = str(payload.get("return_mode", RETURN_AUTO))
        state.downgrade_policy = str(payload.get("downgrade_policy", POLICY_PRIORITY))
        _set_grants(state, payload)
    elif event_type == "grants_set":
        _set_grants(state, payload)
    elif event_type == "policy_set":
        if "return_mode" in payload:
            mode = str(payload["return_mode"])
            if mode not in RETURN_MODES:
                raise QuotaError(f"未知归还模式: {mode}")
            state.return_mode = mode
        if "downgrade_policy" in payload:
            policy = str(payload["downgrade_policy"])
            if policy not in DOWNGRADE_POLICIES:
                raise QuotaError(f"未知降级策略: {policy}")
            state.downgrade_policy = policy
    elif event_type == "priority_set":
        _ensure_entity(state, str(payload["train"]), LEVEL_TRAIN).priority = int(
            payload["priority"]
        )
    elif event_type == "cycle_advanced":
        state.cycle = int(payload["cycle"])
        if payload.get("settlement"):
            state.last_settlement_cycle = state.cycle
        # 新周期刷新发放量。
        for raw in payload.get("grants", ()):  # type: dict
            entity = state.entity(str(raw["entity"]))
            for kind in KIND_NAMES:
                if raw.get(kind) is not None:
                    entity.account(kind).granted = int(raw[kind])
    elif event_type == "permit_issued":
        state.permit_seq = max(state.permit_seq, int(payload.get("seq", 0)))
        permit = Permit(
            id=str(payload["id"]),
            kind=str(payload["kind"]),
            cycle=int(payload["cycle"]),
            train_id=str(payload["train"]),
            platform_id=str(payload["platform"]),
            at=float(payload.get("at", 0.0)),
            sources=tuple(str(item) for item in payload.get("sources", ())),
        )
        state.permits[permit.id] = permit
        train = state.entity(permit.train_id)
        if permit.kind == ARRIVAL:
            train.arrival_seq = int(payload.get("arrival_seq", train.arrival_seq))
        for source in permit.sources:
            holder = permit.train_id if source == "own" else _fund_holder(state, source)
            account = state.entity(holder).account(permit.kind)
            account.held += 1
            account.used_total += 1
    elif event_type == "permit_released":
        permit = state.permits[str(payload["id"])]
        permit.active = False
        permit.released_at = float(payload.get("at", permit.at))
        permit.release_reason = str(payload.get("reason", ""))
        # held 只跟随实际占用：无论资金来自自有还是哪一级贷款，释放时都从
        # 出资方名下回收一次。贷款状态（closed/due/...）只表示债务，不再动 held，
        # 否则会双重扣减。
        for source in permit.sources:
            holder = permit.train_id if source == "own" else _fund_holder(state, source)
            account = state.entity(holder).account(permit.kind)
            account.held = max(0, account.held - 1)
    elif event_type == "loan_opened":
        state.loan_seq = max(state.loan_seq, int(payload.get("seq", 0)))
        loan = Loan(
            id=str(payload["id"]),
            kind=str(payload["kind"]),
            cycle=int(payload["cycle"]),
            borrower=str(payload["borrower"]),
            borrower_level=str(payload["borrower_level"]),
            lender=str(payload["lender"]),
            lender_level=str(payload["lender_level"]),
            permit_id=str(payload["permit"]),
            mode=str(payload.get("mode", RETURN_AUTO)),
            chain=str(payload.get("chain", "")),
        )
        state.loans[loan.id] = loan
    elif event_type == "loan_status_changed":
        loan = state.loans[str(payload["id"])]
        # Loan 是 frozen dataclass，状态流转用替换再放回字典。
        state.loans[loan.id] = Loan(
            **{**loan.as_dict(), "status": str(payload["status"])}
        )
    elif event_type == "train_downgraded":
        state.downgraded_seq = int(payload.get("seq", state.downgraded_seq + 1))
        train = state.entity(str(payload["train"]))
        train.degraded = True
        train.downgrade_reason = str(payload.get("reason", ""))
    elif event_type == "train_restored":
        train = state.entity(str(payload["train"]))
        train.degraded = False
        train.downgrade_reason = ""
        train.restored_cycle = int(payload.get("cycle", state.cycle))
    else:
        raise QuotaError(f"未知配额账本事件: {event_type}")


def fold(transactions: list[LedgerTransaction]) -> QuotaState:
    """把事务序列折叠成台账；未知事件与断裂序列直接拒绝。"""

    state = QuotaState()
    prev_seq = 0
    for txn in transactions:
        if txn.seq != prev_seq + 1:
            raise QuotaError(
                f"配额账本序号不连续：期望 {prev_seq + 1}，实际 {txn.seq}"
            )
        prev_seq = txn.seq
        for event in txn.events:
            _apply(state, event.type, event.payload)
    state.ledger_head = prev_seq
    return state
