"""In-order MQTT acknowledgement tracking. Pure Python, single-threaded use only.

Why this exists:
  * At-least-once: an MQTT message may be acked only after Kafka confirmed its record.
  * MQTT requires PUBACKs to be sent in the order the PUBLISH packets were received
    (MQTT 3.1.1 s4.6 / MQTT 5 s4.6), but Kafka delivery reports for different partitions
    can arrive out of order. So delivered messages wait at the head of a FIFO until every
    message received before them is delivered too.
  * Packet ids are only meaningful on the connection that delivered them. After a
    reconnect the broker re-sends whatever was not acked, so entries from an older
    connection ("generation") are dropped instead of acked -- acking a stale mid could
    acknowledge a different, newer message that reused the id.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class Entry:
    mid: int
    qos: int
    generation: int
    delivered: bool = False


class AckTracker:
    def __init__(self) -> None:
        self._q: deque[Entry] = deque()

    def add(self, mid: int, qos: int, generation: int) -> Entry:
        e = Entry(mid, qos, generation)
        self._q.append(e)
        return e

    @staticmethod
    def mark_delivered(entry: Entry) -> None:
        entry.delivered = True

    def pop_ready(self, current_generation: int) -> tuple[list[tuple[int, int]], int]:
        """Pop the delivered prefix of the queue.

        Returns ([(mid, qos), ...] to ack in order, number of stale entries dropped).
        """
        to_ack: list[tuple[int, int]] = []
        stale = 0
        while self._q and self._q[0].delivered:
            e = self._q.popleft()
            if e.generation != current_generation:
                stale += 1
            elif e.qos > 0:
                to_ack.append((e.mid, e.qos))
        return to_ack, stale

    @property
    def pending(self) -> int:
        """Messages received but not yet acked (delivered or not)."""
        return len(self._q)

    @property
    def undelivered(self) -> int:
        return sum(1 for e in self._q if not e.delivered)
