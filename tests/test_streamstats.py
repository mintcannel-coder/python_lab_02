import csv
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from streamstats import parsers
from streamstats.analysis import LogAnalyzer, filter_by_level, filter_by_source
from streamstats.errors import (
    InvalidEventError,
    InvalidTimestampError,
    UnsupportedFormatError,
)
from streamstats.models import LogEvent
from streamstats.parsers import (
    ParseError,
    get_parser,
    parse_csv_stream,
    parse_jsonl_stream,
    register_parser,
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
    assert event.message == "User logged in"


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
def test_invalid_log_timestamp(bad_ts):
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
        '2026-06-01T10:05:00,ERROR,database,"Connection timeout, retrying"\n',
        encoding = "utf-8",
    )

    events = list(parse_csv_stream(csv_file))
    assert len(events) ==  2
    assert events[0].timestamp == "2026-06-01T10:00:00"
    assert events[0].level == "INFO"
    assert events[0].source == "database"
    assert events[0].message == "Connected successfully"
    assert events[1].level == "ERROR"
    assert events[1].message == "Connection timeout, retrying"


def test_jsonl_parser(tmp_path: Path):
    jsonl_file = tmp_path / "logs.jsonl"
    jsonl_file.write_text(
        '{"timestamp": "2026-06-01T11:00:00", "level": "WARNING", "source": "api", "message": "High latency"}\n'
        '{"timestamp": "2026-06-01T11:01:00", "level": "CRITICAL", "source": "api", "message": "Service down"}\n',
        encoding = "utf-8",
    )

    events = list(parse_jsonl_stream(jsonl_file))
    assert len(events) ==  2
    assert events[0].timestamp == "2026-06-01T11:00:00"
    assert events[0].level == "WARNING"
    assert events[0].source == "api"
    assert events[0].message == "High latency"
    assert events[1].level == "CRITICAL"
    assert events[1].message == "Service down"


def test_empty_csv_with_header_returns_no_events(tmp_path: Path):
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
    assert report["levels"] == {"INFO": 1, "ERROR": 1, "CRITICAL": 1, "WARNING": 1}
    assert report["sources"] == {"db": 2, "auth": 1, "api": 1}
    assert report["top_error_sources"] == {"db": 1, "auth": 1}


def test_csv_parser_returns_parse_error_for_invalid_row(tmp_path: Path) -> None:
    path = tmp_path / "invalid.csv"
    path.write_text(
        "timestamp,level,source,message\n"
        "not-a-date,INFO,svc,message\n",
        encoding="utf-8",
    )

    records = list(parse_csv_stream(path))

    assert len(records) == 1
    assert isinstance(records[0], ParseError)
    assert records[0].line_num == 2
    assert isinstance(records[0].err, InvalidTimestampError)


@pytest.mark.parametrize(
    ("contents", "expected_error"),
    [
        ('{not json}\n', json.JSONDecodeError),
        ('[]\n', AttributeError),
        ('{"timestamp": "bad", "level": "INFO", "source": "svc", "message": "x"}\n', InvalidTimestampError),
    ],
)
def test_jsonl_parser_reports_invalid_records(tmp_path: Path, contents: str, expected_error: type[Exception]) -> None:
    path = tmp_path / "invalid.jsonl"
    path.write_text(contents, encoding="utf-8")

    records = list(parse_jsonl_stream(path))

    assert len(records) == 1
    assert isinstance(records[0], ParseError)
    assert records[0].line_num == 1
    assert isinstance(records[0].err, expected_error)


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
    "--format",
    "csv",
    "--output",
    str(report_json),
    ]
    result = subprocess.run(cmd, check = False, capture_output = True, text = True)

    assert result.returncode ==  0
    assert result.stderr == ""
    assert report_json.exists()
    assert "Отчет успешно сохранен" in result.stdout

    data = json.loads(report_json.read_text(encoding="utf-8"))
    assert data["total_events"] == 2
    assert data["skipped_lines"] == 0
    assert data["levels"] == {"INFO": 1, "ERROR": 1}
    assert data["sources"] == {"web": 2}
    assert data["first_timestamp"] == "2026-06-01T10:00:00"
    assert data["last_timestamp"] == "2026-06-01T10:01:00"

def test_level_and_source_filters_compose_lazily():
    events = [
        LogEvent("2026-06-01T10:00:00", "INFO", "db", "msg1"),
        LogEvent("2026-06-01T11:00:00", "ERROR", "db", "msg2"),
        LogEvent("2026-06-01T12:00:00", "ERROR", "auth", "msg3"),
    ]
    consumed = []

    def event_stream():
        for event in events:
            consumed.append(event.message)
            yield event

    filtered = filter_by_source(filter_by_level(event_stream(), {"ERROR"}), {"db"})
    assert consumed == []

    result = list(filtered)
    assert [event.message for event in result] == ["msg2"]
    assert consumed == ["msg1", "msg2", "msg3"]

def test_parser_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parsers, 'PARSER_REGISTRY', parsers.PARSER_REGISTRY.copy())

    @register_parser('custompipe')
    def parse_pipe_format(file_path: Path) -> Iterator[LogEvent]:
        with open(file_path, 'r', encoding = 'utf-8') as f:
            for line in f:
                parts = line.strip().split('|')
                if len(parts) == 4:
                    yield LogEvent(
                        timestamp=parts[0],
                        level=parts[1],
                        source=parts[2],
                        message=parts[3],
                    )

    custom_file = tmp_path / 'data.pipe'
    custom_file.write_text('2026-06-01T15:00:00|INFO|custom-src|Hello pipe\n', encoding = 'utf-8')

    parser = get_parser(custom_file, format_override = 'custompipe')
    events = list(parser)

    assert len(events) == 1
    assert events[0] == LogEvent(
        timestamp="2026-06-01T15:00:00",
        level="INFO",
        source="custom-src",
        message="Hello pipe",
    )

def test_csv_report_generation(tmp_path: Path):
    analyzer = LogAnalyzer()
    analyzer.add_event(LogEvent('2026-06-01T10:00:00', 'ERROR', 'auth', 'Access denied'))

    report_data = analyzer.to_dict()
    output_csv = tmp_path / 'report.csv'

    save_csv_report(report_data, output_csv)

    assert output_csv.exists()
    with output_csv.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream))

    assert ["total_events", "1"] in rows
    assert ["skipped_lines", "0"] in rows
    assert ["first_timestamp", "2026-06-01T10:00:00"] in rows
    assert ["last_timestamp", "2026-06-01T10:00:00"] in rows
    assert ["ERROR", "1"] in rows
    assert ["auth", "1"] in rows

def test_cli_invalid_line_returns_exit_code_2(tmp_path: Path):
    input_file = tmp_path / "broken.jsonl"
    input_file.write_text(
        '{"timestamp": "2026-06-01T10:00:00", '
        '"level": "INFO", "source": "api", "message": "Started"}\n'
        '{"timestamp": invalid json}\n',
        encoding="utf-8",
    )
    output_file = tmp_path / "report.json"

    cmd = [
        sys.executable,
        "-m",
        "streamstats",
        "analyze",
        str(input_file),
        "--format",
        "jsonl",
        "--output",
        str(output_file),
    ]

    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert str(input_file) in result.stderr
    assert '2' in result.stderr

def test_cli_skip_invalid_line(tmp_path: Path):
    input_file = tmp_path / "partially_broken.jsonl"
    input_file.write_text(
        '{"timestamp": "2026-06-01T10:00:00", '
        '"level": "INFO", "source": "api", "message": "Started"}\n'
        '{"timestamp": invalid json}\n'
        '{"timestamp": "2026-06-01T10:02:00", '
        '"level": "ERROR", "source": "database", "message": "Failed"}\n',
        encoding="utf-8",
    )
    output_file = tmp_path / "report.json"

    cmd = [
        sys.executable,
        "-m",
        "streamstats",
        "analyze",
        str(input_file),
        "--format",
        "jsonl",
        "--output",
        str(output_file),
        "--skip-invalid",
    ]

    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert output_file.exists()
    assert "WARNING" in result.stderr

    report = json.loads(output_file.read_text(encoding="utf-8"))

    assert report["total_events"] == 2
    assert report["levels"]["INFO"] == 1
    assert report["levels"]["ERROR"] == 1
    assert report["skipped_lines"] == 1

def test_cli_aggregates_multiple_input_files(tmp_path: Path):
    first_file = tmp_path / "first.jsonl"
    first_file.write_text(
        '{"timestamp": "2026-06-01T10:00:00", '
        '"level": "INFO", "source": "api", "message": "Started"}\n'
        '{"timestamp": "2026-06-01T10:01:00", '
        '"level": "ERROR", "source": "api", "message": "Failed"}\n',
        encoding="utf-8",
    )

    second_file = tmp_path / "second.jsonl"
    second_file.write_text(
        '{"timestamp": "2026-06-01T11:00:00", '
        '"level": "WARNING", "source": "database", "message": "Slow query"}\n',
        encoding="utf-8",
    )

    output_file = tmp_path / "combined_report.json"

    cmd = [
        sys.executable,
        "-m",
        "streamstats",
        "analyze",
        str(first_file),
        str(second_file),
        "--format",
        "jsonl",
        "--output",
        str(output_file),
    ]

    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert output_file.exists()

    report = json.loads(output_file.read_text(encoding="utf-8"))

    assert report["total_events"] == 3
    assert report["levels"]["INFO"] == 1
    assert report["levels"]["ERROR"] == 1
    assert report["levels"]["WARNING"] == 1

    assert report["sources"]["api"] == 2
    assert report["sources"]["database"] == 1

    assert report["first_timestamp"] == "2026-06-01T10:00:00"
    assert report["last_timestamp"] == "2026-06-01T11:00:00"