from collections.abc import AsyncIterator
from typing import Protocol

from pulsestream.models import RawEvent


class EventSource(Protocol):
    def stream(self) -> AsyncIterator[RawEvent]: ...
