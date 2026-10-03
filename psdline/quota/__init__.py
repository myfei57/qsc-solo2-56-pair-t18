"""分级运行配额。

线路、站台、列车三级各持一份到发额度；下级不够时向上级借，借据下个周期到期，
到期归还（自动扣还或人工确认），还不上就降级。所有变动只追加到账本流水，
配额状态、到发编排与审计读同一份账；冲正用新记录表达，历史不可变。
"""

from __future__ import annotations

from .engine import QuotaEngine
from .ledger import QuotaLedger
from .scopes import (
    ARRIVAL,
    AUTO,
    DEPART,
    KINDS,
    MANUAL,
    KindState,
    Loan,
    line_scope,
    station_scope,
    train_scope,
)

__all__ = [
    "ARRIVAL",
    "AUTO",
    "DEPART",
    "KINDS",
    "MANUAL",
    "KindState",
    "Loan",
    "QuotaEngine",
    "QuotaLedger",
    "line_scope",
    "station_scope",
    "train_scope",
]
