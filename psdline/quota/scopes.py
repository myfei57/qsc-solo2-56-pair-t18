"""分级配额的作用域、份额状态与借据。

配额分线路、站台、列车三级，每级对到站与发车各持一份额度。作用域标识形如
``line:L1`` / ``station:ST1`` / ``train:T01``；借据挂在相邻两级之间：下级
份额不够时向上级借，下个周期归还，还不上就降级。这里只放状态结构与命名规则，
账本读写见 :mod:`psdline.quota.ledger`，增减规则见 :mod:`psdline.quota.engine`。
"""

from __future__ import annotations

from dataclasses import dataclass

ARRIVAL = "arrival"
DEPART = "depart"
KINDS = (ARRIVAL, DEPART)

LINE = "line"
STATION = "station"
TRAIN = "train"
LEVELS = (LINE, STATION, TRAIN)

# 归还方式：周期翻篇自动扣还 / 人工确认归还 / 逾期强制清算。
AUTO = "auto"
MANUAL = "manual"
FORCED = "forced"
REPAY_MODES = (AUTO, MANUAL)

# 借据状态：在途 → 到期待还 → 已归还 / 已降级 / 已冲正。
LOAN_OPEN = "open"
LOAN_DUE = "due"
LOAN_REPAID = "repaid"
LOAN_DEGRADED = "degraded"
LOAN_REFUNDED = "refunded"
LOAN_TERMINAL = (LOAN_REPAID, LOAN_DEGRADED, LOAN_REFUNDED)


def line_scope(line_id: str) -> str:
    """返回线路作用域标识。"""

    return f"{LINE}:{line_id}"


def station_scope(station_id: str) -> str:
    """返回站台作用域标识。"""

    return f"{STATION}:{station_id}"


def train_scope(train_id: str) -> str:
    """返回列车作用域标识。"""

    return f"{TRAIN}:{train_id}"


def level_of(scope: str) -> str:
    """返回作用域所属层级（``line`` / ``station`` / ``train``）。"""

    return scope.partition(":")[0]


def name_of(scope: str) -> str:
    """返回作用域去掉层级前缀后的业务标识。"""

    return scope.partition(":")[2]


@dataclass
class KindState:
    """一个作用域在单一到发类别上的周期份额。

    ``debt_hold`` 是本期被抵债冻结的份额：借入方归还时，还款直接从新周期份额
    里扣掉，表现为可用份额减少；降级时整份冻结。``lent`` 只是未结清借据的台账
    （借出总是随单发生，对应的消费已经计入 ``used``），不重复扣减可用额度。
    """

    limit: int = 0
    used: int = 0
    lent: int = 0
    borrowed: int = 0
    debt_hold: int = 0

    @property
    def remaining(self) -> int:
        """返回本期还可支配的份额（抵债冻结不可用，借入可用）。"""

        return self.limit - self.debt_hold - self.used + self.borrowed

    def as_dict(self) -> dict:
        return {
            "limit": self.limit,
            "used": self.used,
            "lent": self.lent,
            "borrowed": self.borrowed,
            "debt_hold": self.debt_hold,
            "remaining": self.remaining,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "KindState":
        return cls(
            limit=int(payload.get("limit", 0)),
            used=int(payload.get("used", 0)),
            lent=int(payload.get("lent", 0)),
            borrowed=int(payload.get("borrowed", 0)),
            debt_hold=int(payload.get("debt_hold", 0)),
        )


@dataclass
class Loan:
    """一笔借据：下级向上级借出的份额，下个周期到期。"""

    id: str
    borrower: str
    lender: str
    kind: str
    amount: int
    open_amount: int
    mode: str
    status: str
    created_period: int
    due_period: int
    created_at: float

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "borrower": self.borrower,
            "lender": self.lender,
            "kind": self.kind,
            "amount": self.amount,
            "open_amount": self.open_amount,
            "mode": self.mode,
            "status": self.status,
            "created_period": self.created_period,
            "due_period": self.due_period,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "Loan":
        return cls(
            id=str(payload.get("id", "")),
            borrower=str(payload.get("borrower", "")),
            lender=str(payload.get("lender", "")),
            kind=str(payload.get("kind", ARRIVAL)),
            amount=int(payload.get("amount", 0)),
            open_amount=int(payload.get("open_amount", 0)),
            mode=str(payload.get("mode", AUTO)),
            status=str(payload.get("status", LOAN_OPEN)),
            created_period=int(payload.get("created_period", 0)),
            due_period=int(payload.get("due_period", 0)),
            created_at=float(payload.get("created_at", 0.0)),
        )
