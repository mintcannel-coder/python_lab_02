import json
import struct
import subprocess
import sys
import threading
import time
import tracemalloc
from collections.abc import Iterator
from pathlib import Path

import pytest

from streamstats.analysis import LogAnalyzer, filter_by_level, filter_by_source
from streamstats.errors import (
    InvalidEventError,
    InvalidTimestampError,
    UnsupportedFormatError,
)
from streamstats.models import LogEvent
from streamstats.parsers import (
    get_parser,
    parse_binary_stream,
    parse_csv_stream,
    parse_jsonl_stream,
    register_parser,
    tail_jsonl_stream,
)
from streamstats.report import save_csv_report


def test_valid_log_event():
    event = LogEvent(
        timestamp = "2026-06-01T12:00:00",
        level = "INFO",
        source = "auth-service",
        message = "User logged in",
    )
    assert event.level ==  "INFO"
    assert event.source ==  "auth-service"
    assert event.timestamp ==  "2026-06-01T12:00:00"


@pytest.mark.parametrize("bad_level", ["DEBUGG", "UNKNOWN", "info", ""])
def test_invalid_log_level(bad_level):
    with pytest.raises(InvalidEventError):
        LogEvent(
            timestamp = "2026-06-01T12:00:00",
            level = bad_level,
            source = "service",
            message = "msg",
        )


@pytest.mark.parametrize("bad_source", ["", "   ", None])
def test_invalid_log_source(bad_source):
    with pytest.raises(InvalidEventError):
        LogEvent(
            timestamp = "2026-06-01T12:00:00",
            level = "ERROR",
            source = bad_source,
            message = "msg",
        )


@pytest.mark.parametrize("bad_ts", ["not-a-date", "2026-13-01T00:00:00"])
def test_invalid_timestamp(bad_ts):
    with pytest.raises(InvalidTimestampError):
        LogEvent(
            timestamp = bad_ts,
            level = "WARNING",
            source = "service",
            message = "msg",
        )


def test_csv_parser(tmp_path: Path):
    csv_file = tmp_path / "logs.csv"
    csv_file.write_text(
        "timestamp,level,source,message\n"
        "2026-06-01T10:00:00,INFO,database,Connected successfully\n"
        "2026-06-01T10:05:00,ERROR,database,Connection timeout\n",
        encoding = "utf-8",
    )

    events = list(parse_csv_stream(csv_file))
    assert len(events) ==  2
    assert events[0].level ==  "INFO"
    assert events[0].source ==  "database"
    assert events[1].level ==  "ERROR"


def test_jsonl_parser(tmp_path: Path):
    jsonl_file = tmp_path / "logs.jsonl"
    jsonl_file.write_text(
        '{"timestamp": "2026-06-01T11:00:00", "level": "WARNING", "source": "api", "message": "High latency"}\n'
        '{"timestamp": "2026-06-01T11:01:00", "level": "CRITICAL", "source": "api", "message": "Service down"}\n',
        encoding = "utf-8",
    )

    events = list(parse_jsonl_stream(jsonl_file))
    assert len(events) ==  2
    assert events[0].level ==  "WARNING"
    assert events[1].level ==  "CRITICAL"


def test_empty_file(tmp_path: Path):
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("timestamp,level,source,message\n", encoding = "utf-8")

    events = list(parse_csv_stream(empty_csv))
    assert len(events) ==  0


def test_unicode_support(tmp_path: Path):
    jsonl_file = tmp_path / "unicode.jsonl"
    jsonl_file.write_text(
        '{"timestamp": "2026-06-01T12:00:00", "level": "INFO", "source": "модуль-авторизации", "message": "Привет, мир!"}\n',
        encoding = "utf-8",
    )

    events = list(parse_jsonl_stream(jsonl_file))
    assert len(events) ==  1
    assert events[0].source ==  "модуль-авторизации"
    assert events[0].message ==  "Привет, мир!"


def test_log_analyzer_aggregation():
    analyzer = LogAnalyzer()
    analyzer.add_event(LogEvent("2026-06-01T10:00:00", "INFO", "db", "ok"))
    analyzer.add_event(LogEvent("2026-06-01T12:00:00", "ERROR", "db", "err1"))
    analyzer.add_event(LogEvent("2026-06-01T11:00:00", "CRITICAL", "auth", "err2"))
    analyzer.add_event(LogEvent("2026-06-01T09:00:00", "WARNING", "api", "warn"))

    report = analyzer.to_dict()
    assert report["total_events"] ==  4
    assert report["first_timestamp"] ==  "2026-06-01T09:00:00"
    assert report["last_timestamp"] ==  "2026-06-01T12:00:00"
    assert report["levels"]["ERROR"] ==  1
    assert report["levels"]["CRITICAL"] ==  1
    assert "db" in report["top_error_sources"]
    assert "auth" in report["top_error_sources"]


def test_unsupported_format(tmp_path: Path):
    bad_file = tmp_path / "data.txt"
    bad_file.write_text("some data", encoding = "utf-8")

    with pytest.raises(UnsupportedFormatError):
        get_parser(bad_file)


def test_cli_integration(tmp_path: Path):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "timestamp,level,source,message\n"
        "2026-06-01T10:00:00,INFO,web,Started\n"
        "2026-06-01T10:01:00,ERROR,web,Failed\n",
        encoding = "utf-8",
    )
    report_json = tmp_path / "report.json"

    cmd = [
        sys.executable,
        "-m",
        "streamstats",
        "analyze",
        str(input_csv),
        str(report_json),
    ]
    result = subprocess.run(cmd, check = False, capture_output = True, text = True)

    assert result.returncode ==  0
    assert report_json.exists()

    data = json.loads(report_json.read_text(encoding = "utf-8"))
    assert data["total_events"] ==  2
    assert data["levels"]["INFO"] ==  1
    assert data["levels"]["ERROR"] ==  1

def test_lazy_filters():
    events = [
        LogEvent("2026-06-01T10:00:00", "INFO", "db", "msg1"),
        LogEvent("2026-06-01T11:00:00", "ERROR", "db", "msg2"),
        LogEvent("2026-06-01T12:00:00", "ERROR", "auth", "msg3"),
    ]

    filtered = filter_by_level(iter(events), {"ERROR"})
    filtered = filter_by_source(filtered, {"db"})
    
    result = list(filtered)
    assert len(result) ==  1
    assert result[0].message ==  "msg2"

def test_parser_registry(tmp_path: Path):
    @register_parser('custompipe')
    def parse_pipe_format(file_path: Path) -> Iterator[LogEvent]:
        with open(file_path, 'r', encoding = 'utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) ==  4:
                    yield LogEvent(
                        timestamp = parts[0],
                        level = parts[1],
                        source = parts[2],
                        message = parts[3]
                    )

    custom_file = tmp_path / 'data.pipe'
    custom_file.write_text('2026-06-01T15:00:00|INFO|custom-src|Hello pipe\n', encoding = 'utf-8')

    parser = get_parser(custom_file, format_override = 'custompipe')
    events = list(parser)

    assert len(events) ==  1
    assert events[0].source ==  'custom-src'
    assert events[0].message ==  'Hello pipe'

def test_csv_report_generation(tmp_path: Path):
    analyzer = LogAnalyzer()
    analyzer.add_event(LogEvent('2026-06-01T10:00:00', 'ERROR', 'auth', 'Access denied'))

    report_data = analyzer.to_dict()
    output_csv = tmp_path / 'report.csv'

    save_csv_report(report_data, output_csv)

    assert output_csv.exists()
    content = output_csv.read_text(encoding = 'utf-8')

    assert 'total_events' in content
    assert 'ERROR' in content
    assert 'auth' in content

def test_memory_usage_on_large_stream(tmp_path: Path):
    large_file = tmp_path / 'large_file.csv'

    with open(large_file, 'w', encoding = 'utf-8') as f:
        f.write('timestamp,level,source,message\n')
        f.writelines(f'2026-06-01T10:00:00,INFO,service-{i % 10},Message number {i}\n' for i in range(100_000))

    tracemalloc.start()

    analyzer = LogAnalyzer()
    event_stream = parse_csv_stream(large_file)
    for event in event_stream:
        analyzer.add_event(event)

    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_usage = peak / 1024 / 1024

    assert peak_usage < 20.0, f'Потребление памяти слишком высокое: {peak_usage: .2f} MB'

def test_tail_f_iterator(tmp_path: Path):
    log_file = tmp_path / 'live.jsonl'
    log_file.write_text('', encoding = 'utf-8')

    recieved_events = []

    def background_reader():
        for event in tail_jsonl_stream(log_file, poll_interval = 0.05):
            recieved_events.append(event)
            if len(recieved_events) >= 2:
                break

    thread = threading.Thread(target = background_reader, daemon = True)
    thread.start()

    time.sleep(0.1)

    with open(log_file, 'a', encoding = 'utf-8') as f:
        f.write('{"timestamp": "2026-06-01T12:00:00", "level": "INFO", "source": "app", "message": "First live"}\n')

    time.sleep(0.1)

    with open(log_file, 'a', encoding = 'utf-8') as f:
        f.write('{"timestamp": "2026-06-01T12:00:00", "level": "ERROR", "source": "app", "message": "Second live"}\n')

    thread.join(timeout = 2.0)

    assert len(recieved_events) ==  2
    assert recieved_events[0].message ==  'First live'
    assert recieved_events[1].message ==  'Second live'

def test_binary_memoryview_parser(tmp_path: Path):
    bin_file = tmp_path / 'logs.bin'

    payload = json.dumps({
        'timestamp': '2026-06-01T14:00:00',
        'level': 'CRITICAL',
        'source': 'kernel',
        'message': 'Memory fault'
    }).encode('utf-8')

    length_header = struct.pack('>I', len(payload))

    bin_file.write_bytes(length_header + payload)

    events = list(parse_binary_stream(bin_file))

    assert len(events) ==  1
    assert events[0].level ==  'CRITICAL'
    assert events[0].source ==  'kernel'
    assert events[0].message ==  'Memory fault'