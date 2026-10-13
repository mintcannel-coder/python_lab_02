from collections import Counter
from collections.abc import Iterator
from datetime import datetime

from .models import LogEvent


class LogAnalyzer:
    def __init__(self):
        self.total_events = 0
        self.levels_counter = Counter()
        self.sources_counter = Counter()
        self.error_sources_counter = Counter()
        
        self.min_timestamp: datetime | None = None
        self.max_timestamp: datetime | None = None
        self.skipped_lines = 0

    def add_event(self, event: LogEvent):
        self.total_events +=  1
        self.levels_counter[event.level] +=  1
        self.sources_counter[event.source] +=  1

        if event.level in {'ERROR', 'CRITICAL'}:
            self.error_sources_counter[event.source] +=  1

        dt = datetime.fromisoformat(event.timestamp)
        if self.min_timestamp is None or dt < self.min_timestamp:
            self.min_timestamp = dt
        if self.max_timestamp is None or dt > self.max_timestamp:
            self.max_timestamp = dt

    def to_dict(self) -> dict:
        return {
            'total_events': self.total_events,
            'skipped_lines': self.skipped_lines,
            'levels': dict(self.levels_counter),
            'sources': dict(self.sources_counter),
            'top_error_sources': dict(self.error_sources_counter.most_common(5)),
            'first_timestamp': self.min_timestamp.isoformat() if self.min_timestamp else None,
            'last_timestamp': self.max_timestamp.isoformat() if self.max_timestamp else None,
        }

def filter_by_level(events: Iterator[LogEvent], levels: set[str]) -> Iterator[LogEvent]:
    for event in events:
        if event.level in levels:
            yield event

def filter_by_source(events: Iterator[LogEvent], sources: set[str]) -> Iterator[LogEvent]:
    for event in events:
        if event.source in sources:
            yield event

def filter_by_time(
    events: Iterator[LogEvent], 
    start_time: str | None = None, 
    end_time: str | None = None
) -> Iterator[LogEvent]:
    start_dt = datetime.fromisoformat(start_time) if start_time else None
    end_dt = datetime.fromisoformat(end_time) if end_time else None

    for event in events:
        event_dt = datetime.fromisoformat(event.timestamp)
        if start_dt and event_dt < start_dt:
            continue
        if end_dt and event_dt > end_dt:
            continue
        yield event