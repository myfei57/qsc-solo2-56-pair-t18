"""运行配额。

包含两代实现：

* :class:`QuotaRegistry` —— 早期的线路级"到站/发车两个总上限"，保留以兼容旧
  接口与旧用例；
* :class:`QuotaBook` —— 分级配额（线路/站台/列车各一份额度），站台与列车额度
  不够时沿层级向上借，下个周期窗口归还，还不上按策略降级。所有动作落在只追加、
  哈希链串联的 :class:`QuotaLedger` 上，配额台账、到发编排、审计读同一份账。
"""

from __future__ import annotations

from .book import Grant, QuotaBook
from .ledger import LedgerEvent, LedgerTransaction, QuotaLedger
from .model import (
    ARRIVAL,
    DEPART,
    DOWNGRADE_POLICIES,
    LEVEL_LINE,
    LEVEL_PLATFORM,
    LEVEL_TRAIN,
    LOAN_CLOSED,
    LOAN_DEFAULTED,
    LOAN_DUE,
    LOAN_OPEN,
    POLICY_ARRIVAL,
    POLICY_PRIORITY,
    RETURN_AUTO,
    RETURN_MANUAL,
    EntityState,
    KindAccount,
    Loan,
    Permit,
    QuotaError,
    QuotaState,
)
from .quota import Quota, QuotaRegistry

__all__ = [
    "ARRIVAL",
    "DEPART",
    "DOWNGRADE_POLICIES",
    "LEVEL_LINE",
    "LEVEL_PLATFORM",
    "LEVEL_TRAIN",
    "LOAN_CLOSED",
    "LOAN_DEFAULTED",
    "LOAN_DUE",
    "LOAN_OPEN",
    "POLICY_ARRIVAL",
    "POLICY_PRIORITY",
    "RETURN_AUTO",
    "RETURN_MANUAL",
    "EntityState",
    "Grant",
    "KindAccount",
    "LedgerEvent",
    "LedgerTransaction",
    "Loan",
    "Permit",
    "Quota",
    "QuotaBook",
    "QuotaError",
    "QuotaLedger",
    "QuotaRegistry",
    "QuotaState",
]
