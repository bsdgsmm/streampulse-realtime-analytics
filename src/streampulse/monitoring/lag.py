"""Consumer lag: broker high watermark minus the group's committed offset."""

from __future__ import annotations

from dataclasses import dataclass

NO_COMMITTED_OFFSET = -1001


@dataclass(frozen=True)
class PartitionLag:
    topic: str
    partition: int
    committed_offset: int
    high_watermark: int
    low_watermark: int

    @property
    def lag(self) -> int:
        """Messages not yet committed by the group.

        Without a commit the whole retained range counts as lag.
        """
        start = self.low_watermark if self.committed_offset < 0 else self.committed_offset
        return max(self.high_watermark - start, 0)


def total_lag(partitions: list[PartitionLag]) -> int:
    return sum(item.lag for item in partitions)
