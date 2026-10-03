"""分级配额接入到发编排：账本即唯一事实来源，平台/控制台/审计共读一份。"""

from __future__ import annotations

import json

import pytest

from psdline.app import build_platform
from psdline.config import (
    PlatformConfig,
    PlatformSection,
    QuotaSection,
    default_config,
)
from psdline.console.api import Api
from psdline.quota import RETURN_MANUAL


def _tight_config(mode: str = "auto") -> PlatformConfig:
    base = default_config()
    quota = QuotaSection(
        arrival_limit=3,
        depart_limit=3,
        platform_arrival_limit=1,
        platform_depart_limit=1,
        train_arrival_limit=0,
        train_depart_limit=0,
        return_mode=mode,
    )
    from dataclasses import replace

    return replace(base, quota=quota)


def test_arrive_depart_share_one_ledger(tmp_path, clock):
    platform = build_platform(
        _tight_config(), clock=clock, storage_root=str(tmp_path / "state")
    )
    platform.start()
    for train_id in ("T1", "T2", "T3"):
        outcome = platform.arrive(train_id, "ST1", 0)
        assert outcome["ok"] is True
    # 线路硬总车位 3：第四列拒绝，且平台给出违规原因。
    fourth = platform.arrive("T4", "ST1", 0)
    assert fourth["ok"] is False
    assert any("配额" in reason for reason in fourth["violations"])

    # 台账、到发编排读到的是同一份账：账本活动占用数 == 物理在站 3。
    hierarchy = platform.quota_status()
    assert hierarchy["active_arrivals"] == 3

    # 发车后归还到站占用，后续车能补进来。
    platform.check_gap("T1")
    assert platform.depart("T1", "ST1")["ok"] is True
    assert platform.arrive("T4", "ST1", 0)["ok"] is True
    assert platform.quota_status()["active_arrivals"] == 3
    assert platform.quota_ledger.verify()["ok"] is True


def test_quota_state_survives_restart(tmp_path, clock):
    root = str(tmp_path / "state")
    first = build_platform(_tight_config(), clock=clock, storage_root=root)
    first.start()
    first.arrive("T1", "ST1", 0)
    head = first.quota_ledger.head()[0]
    assert head >= 1

    second = build_platform(_tight_config(), clock=clock, storage_root=root)
    assert second.quota_book.state.ledger_head == head
    assert "T1" in second.quota_book.state.entities
    # 重启后额度判断依旧成立：3 个车位，T1 占 1，还能进 2 个。
    assert second.arrive("T2", "ST1", 0)["ok"] is True
    assert second.arrive("T3", "ST1", 0)["ok"] is True
    assert second.arrive("T4", "ST1", 0)["ok"] is False


def test_failed_arrival_rolls_back_quota(tmp_path, clock):
    platform = build_platform(
        _tight_config(), clock=clock, storage_root=str(tmp_path / "state")
    )
    platform.start()
    # 对位失败（position 超公差）会在配额占用之后回滚该占用。
    outcome = platform.arrive("TBAD", "ST1", 999999)
    assert outcome["ok"] is False
    assert platform.quota_status()["active_arrivals"] == 0
    # 账本头里不应留下 permit_issued 未被释放的占用（回滚事件成对）。
    permits = [
        permit
        for permit in platform.quota_book.state.permits.values()
        if permit.train_id == "TBAD" and permit.active
    ]
    assert permits == []


def test_quota_api_endpoints(tmp_path, clock):
    platform = build_platform(
        _tight_config(), clock=clock, storage_root=str(tmp_path / "state")
    )
    platform.start()
    platform.arrive("T1", "ST1", 0)
    api = Api(platform)

    status, payload = api.handle("GET", "/api/quota/hierarchy", {}, {})
    assert status == 200 and payload["hierarchical"]["active_arrivals"] == 1
    assert payload["ledger"]["verify"]["ok"] is True

    status, payload = api.handle("GET", "/api/quota/audit", {}, {})
    assert status == 200 and payload["entries"]
    kinds = {
        event["type"]
        for entry in payload["entries"]
        for event in entry["detail"]["events"]
    }
    assert "permit_issued" in kinds

    status, payload = api.handle("POST", "/api/quota/settle", {}, {})
    assert status == 200 and payload["cycle"] == 1

    # readiness 把账本完整性纳入健康检查。
    status, ready = api.handle("GET", "/api/readiness", {}, {})
    assert ready["checks"]["quota_ledger"] is True


def test_policy_switch_and_manual_confirm_via_platform(tmp_path, clock):
    platform = build_platform(
        _tight_config(mode="manual"), clock=clock, storage_root=str(tmp_path / "state")
    )
    platform.start()
    platform.arrive("T1", "ST1", 0)
    platform.check_gap("T1")
    platform.depart("T1", "ST1")  # manual：释放后贷款挂 due
    due = platform.quota_book.state.open_loans(status="due")
    assert due
    result = platform.confirm_quota_return()
    assert result["count"] >= 1
    assert platform.quota_ledger.verify()["ok"] is True
