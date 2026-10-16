import json
import os
import struct
import threading
import time
import tracemalloc
from pathlib import Path
from queue import Queue

import pytest

from streamstats.analysis import LogAnalyzer, filter_by_time
from streamstats.errors import InvalidEventError, StreamStatsError
from streamstats.models import LogEvent
from streamstats.parsers import (
    parse_binary_stream,
    parse_csv_stream,
    parse_jsonl_stream,
    tail_jsonl_stream,
)

MIB = 1024 * 1024
HEAVY_FILE_SIZE = 500 * MIB


def _make_event_data(index: int, *, message: str | None = None) -> dict[str, str]:
    return {
        'timestamp': f'2026-06-01T10:{(index // 60) % 60:02d}:{index % 60:02d}',
        'level': 'ERROR' if index % 10 == 0 else 'INFO',
        'source': f'service-{index % 8}',
        'message': message if message is not None else f'event-{index}',
    }

def test_empty_jsonl_file_returns_no_events(tmp_path: Path) -> None:
    path = tmp_path / 'empty.jsonl'
    path.write_text('', encoding='utf-8')

    assert list(parse_jsonl_stream(path)) == []

def test_time_filter_includes_both_boundaries() -> None:
    events = [
        LogEvent('2026-06-01T10:00:00', 'INFO', 'svc', 'at-start'),
        LogEvent('2026-06-01T10:30:00', 'ERROR', 'svc', 'inside'),
        LogEvent('2026-06-01T11:00:00', 'INFO', 'svc', 'at-end'),
        LogEvent('2026-06-01T11:00:01', 'INFO', 'svc', 'after-end'),
    ]

    result = list(
        filter_by_time(
            iter(events),
            start_time='2026-06-01T10:00:00',
            end_time='2026-06-01T11:00:00',
        )
    )
    assert [event.message for event in result] == ['at-start', 'inside', 'at-end']


@pytest.mark.skipif(
    os.environ.get('STREAMSTATS_RUN_HEAVY') != '1',
    reason='ресурсоёмкий тест: установите STREAMSTATS_RUN_HEAVY=1 для запуска',
)
def test_heavy_csv_streaming_memory_500_mib(tmp_path: Path) -> None:
     
    csv_path = tmp_path / 'heavy_500_mib.csv'
    message = 'x' * 900
    row_template = f'2026-06-01T10:00:00,INFO,service-0,{message}\n'
    header = 'timestamp,level,source,message\n'
    rows_per_block = 10_000
    block = row_template * rows_per_block

    with csv_path.open('w', encoding='utf-8', newline='') as stream:
        stream.write(header)
        while csv_path.stat().st_size < HEAVY_FILE_SIZE:
            stream.write(block)

    actual_size = csv_path.stat().st_size
    assert actual_size >= HEAVY_FILE_SIZE, (
        f'Тестовый файл слишком мал: {actual_size / MIB:.1f} MiB; нужно >= 500 MiB'
    )

    analyzer = LogAnalyzer()
    tracemalloc.start()
    try:
        for item in parse_csv_stream(csv_path):
            if isinstance(item, LogEvent):
                analyzer.add_event(item)
            else:
                analyzer.skipped_lines += 1
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    peak_mib = peak_bytes / MIB
    report = analyzer.to_dict()
    assert report['total_events'] > 0
    assert report['skipped_lines'] == 0
    assert peak_mib < 64, (
        f'Пик Python-памяти {peak_mib:.1f} MiB при файле {actual_size / MIB:.1f} MiB; '
        'возможна загрузка файла/событий целиком в память'
    )


def _start_tail_reader(path: Path, expected_events: int):
    events: Queue = Queue()
    failures: Queue = Queue()

    def consume() -> None:
        try:
            count = 0
            for count, event in enumerate(tail_jsonl_stream(path, poll_interval=0.01)):
                events.put(event)
                if count >= expected_events:
                    return
        except StreamStatsError as exc:
            failures.put(exc)

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    return events, failures, thread


def test_tail_reads_only_complete_jsonl_records(tmp_path: Path) -> None:
    path = tmp_path / 'live_partial.jsonl'
    path.write_text('', encoding='utf-8')
    received, failures, thread = _start_tail_reader(path, expected_events=1)

    time.sleep(0.05)
    record = json.dumps(_make_event_data(1), ensure_ascii=False)
    with path.open('a', encoding='utf-8') as stream:
        stream.write(record[: len(record) // 2])
        stream.flush()
    time.sleep(0.08)

    assert received.empty(), 'tail выдал неполную JSONL-запись до символа новой строки'
    assert failures.empty(), f'tail упал на частичной записи: {failures.get_nowait() if not failures.empty() else ""}'

    with path.open('a', encoding='utf-8') as stream:
        stream.write(record[len(record) // 2 :] + '\n')
        stream.flush()

    event = received.get(timeout=2)
    assert event.message == 'event-1'
    assert event.source == 'service-1'
    assert failures.empty()
    assert thread.is_alive()


def test_tail_follows_multiple_appends_in_order(tmp_path: Path) -> None:
    path = tmp_path / 'live_appends.jsonl'
    path.write_text("{'timestamp':'2026-06-01T09:00:00','level':'INFO','source':'old','message':'old'}\n",
        encoding='utf-8',
    )
    received, failures, thread = _start_tail_reader(path, expected_events=2)

    time.sleep(0.05)
    assert received.empty(), 'tail прочитал старую строку вместо только новых'

    for index in range(2, 4):
        with path.open('a', encoding='utf-8') as stream:
            stream.write(json.dumps(_make_event_data(index)) + '\n')
            stream.flush()
        event = received.get(timeout=2)
        assert event.message == f'event-{index}'

    assert failures.empty()
    assert thread.is_alive()


def test_binary_parser_handles_large_payload(tmp_path: Path) -> None:
    path = tmp_path / 'large_payload.bin'
    data = _make_event_data(7, message='z' * (2 * MIB))
    payload = json.dumps(data, ensure_ascii=False).encode('utf-8')
    path.write_bytes(struct.pack('>I', len(payload)) + payload)

    events = list(parse_binary_stream(path))
    assert len(events) == 1
    assert events[0].timestamp == data['timestamp']
    assert events[0].level == data['level']
    assert events[0].source == data['source']
    assert events[0].message == data['message']


@pytest.mark.parametrize(
    ('contents', 'error_match'),
    [
        (b'\x00\x00', 'заголовка'),
        (struct.pack('>I', 10) + b'{}', 'короче заявленной длины'),
        (struct.pack('>I', 3) + b'\xff\xfe\xfd', 'парсинга бинарного пакета'),
        (struct.pack('>I', 1) + b'{', 'парсинга бинарного пакета'),
    ],
)
def test_binary_parser_rejects_truncated_or_invalid_packet(
    tmp_path: Path, contents: bytes, error_match: str
) -> None:
    path = tmp_path / 'broken.bin'
    path.write_bytes(contents)
    with pytest.raises(InvalidEventError, match=error_match):
        list(parse_binary_stream(path))


def test_binary_parser_reads_multiple_concatenated_packets(tmp_path: Path) -> None:
    path = tmp_path / 'two_packets.bin'
    packets = []
    for index in range(2):
        payload = json.dumps(_make_event_data(index)).encode('utf-8')
        packets.append(struct.pack('>I', len(payload)) + payload)
    path.write_bytes(b''.join(packets))

    events = list(parse_binary_stream(path))
    assert [event.message for event in events] == ['event-0', 'event-1']
