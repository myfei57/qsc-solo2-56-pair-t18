"""配额账本：只追加的流水、快照与崩溃恢复。

配额、到发编排与审计读同一份账：所有配额变动（配置、消费、借入、归还、降级、
冲正）都先落成一条流水记录（单行写入并 fsync）再改内存状态，任何组件看到的
都是从这条流水推出的同一份视图。流水只追加不重写，撤销与回滚用新的冲正记录
表达，历史不可变。

崩溃安全：追加是单行原子写入，掉电最多在最后一段尾部留下半行未提交的记录。
打开账本时若末段末行无法解析，把它当作未提交的写入截断（截断后 fsync），
其余段出现坏行一律视为存储损坏，抛 :class:`~psdline.errors.StorageError`。
快照只是检查点：崩溃后先取快照，再重放快照之后的流水尾部，二者任一缺失都
退化为全量重放，结果一致。
"""

from __future__ import annotations

import json
import os
import threading

from ..clock import Clock
from ..errors import StorageError
from ..store import JsonStore, JsonlJournal


class QuotaLedger:
    """配额流水账：``quota-*.jsonl`` 分段追加 + ``quota/snapshot`` 检查点。"""

    SNAPSHOT_DOC = "quota/snapshot"

    def __init__(
        self,
        store: JsonStore,
        clock: Clock,
        topic: str = "quota",
        segment_limit: int = 512,
    ) -> None:
        self._store = store
        self._clock = clock
        self._topic = topic
        self._segment_limit = max(1, segment_limit)
        self._lock = threading.RLock()
        self._repair_tail()
        self._journal = JsonlJournal(store.root, topic, segment_limit=self._segment_limit)
        self._seq = 0
        for record in self.records():
            self._seq = max(self._seq, int(record.get("seq", 0)))

    @property
    def seq(self) -> int:
        """返回已落账的最大序号（下一条记录的序号是它加一）。"""

        with self._lock:
            return self._seq

    def _segment_path(self, index: int) -> str:
        return os.path.join(self._store.root, f"{self._topic}-{index:05d}.jsonl")

    def _repair_tail(self) -> None:
        """截掉末段尾部未提交的半行；其余段的坏行直接判损坏。"""

        segments: list[str] = []
        for index in range(4096):
            path = self._segment_path(index)
            if not os.path.isfile(path):
                break
            segments.append(path)
        for position, path in enumerate(segments):
            with open(path, "rb") as handle:
                data = handle.read()
            if not data:
                continue
            chunks = data.split(b"\n")
            tail = chunks[-1]
            body = chunks[:-1]
            for raw in body:
                if not raw.strip():
                    continue
                try:
                    json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise StorageError(
                        f"配额流水 {path} 已损坏: {error}", component="quota"
                    ) from error
            if not tail.strip():
                continue
            try:
                json.loads(tail.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                if position != len(segments) - 1:
                    raise StorageError(f"配额流水 {path} 已损坏: 末行残缺", component="quota")
                good = len(data) - len(tail)
                try:
                    with open(path, "r+b") as handle:
                        handle.truncate(good)
                        handle.flush()
                        os.fsync(handle.fileno())
                except OSError as error:
                    raise StorageError(
                        f"截断配额流水 {path} 未提交尾部失败: {error}", component="quota"
                    ) from error

    def append(self, op: str, period: int, **payload: object) -> dict:
        """追加一条记录并 fsync，返回落账后的完整记录。"""

        if not op:
            raise StorageError("账本记录类型为空", component="quota")
        with self._lock:
            record = {
                "seq": self._seq + 1,
                "id": f"qrec-{self._seq + 1:06d}",
                "at": self._clock.now(),
                "period": period,
                "op": op,
                **payload,
            }
            self._journal.append(record)
            self._seq += 1
            return record

    def records(self) -> list[dict]:
        """按落账顺序返回全部记录（全量重放入口）。"""

        return self._journal.read_all()

    def tail(self, limit: int = 50) -> list[dict]:
        """返回最近的若干条记录，供审计与控制台读取同一份账。"""

        if limit <= 0:
            return []
        return self.records()[-limit:]

    def stats(self) -> dict:
        """返回账本物理状态（记录数、分段数、当前段）。"""

        return {
            "topic": self._topic,
            "records": self._journal.count(),
            "segments": len(self._journal.segments()),
            "active_segment": self._journal.active_segment(),
        }

    def save_snapshot(self, state: dict) -> None:
        """把引擎状态连同已应用到的最大序号落成检查点（原子替换）。"""

        with self._lock:
            self._store.write(self.SNAPSHOT_DOC, {"seq": self._seq, "state": state})

    def load_snapshot(self) -> tuple[int, dict] | None:
        """读取检查点；缺失或损坏时返回 ``None``，由调用方全量重放。"""

        try:
            raw = self._store.read(self.SNAPSHOT_DOC)
        except StorageError:
            return None
        if not isinstance(raw, dict):
            return None
        state = raw.get("state")
        if not isinstance(state, dict):
            return None
        return int(raw.get("seq", 0)), state
