import csv
import json
import struct
import time
from collections.abc import Callable, Iterator
from os import SEEK_END
from pathlib import Path

from .errors import InvalidEventError, UnsupportedFormatError
from .models import LogEvent

PARSER_REGISTRY: dict[str, Callable[[Path], Iterator[LogEvent]]] = {}

def register_parser(format_name: str):
    def decorator(func: Callable[[Path], Iterator[LogEvent]]):
        PARSER_REGISTRY[format_name.lower()] = func
        return func
    return decorator

def parse_binary_stream(file_path: Path) -> Iterator[LogEvent]:
    with open(file_path, 'rb') as f:
        content = f.read()

    mv = memoryview(content)
    offset = 0

    while offset < len(mv):
        if len(mv) - offset < 4:
            break

        pkg_len = struct.unpack('>I', bytes(mv[offset : offset + 4]))[0]
        offset +=  4

        if len(mv) - offset < pkg_len:
            break

        packet_mv = mv[offset : offset + pkg_len]
        offset +=  pkg_len

        try:
            packet_str = bytes(packet_mv).decode('utf-8')
            data = json.loads(packet_str) if 'json' in globals() else {}

            yield LogEvent(
                timestamp = data.get('timestamp', '2026-06-01T00:00:00'),
                level = data.get('level', 'INFO'),
                source = data.get('source', 'binary-source'),
                message = data.get('message', '')
            )
        except Exception as e:
            raise InvalidEventError(f'Ошибка парсинга бинарного пакета: {e}') from e

@register_parser('csv')
def parse_csv_stream(file_path: Path) -> Iterator[LogEvent]:
    with open(file_path, 'r', encoding = 'utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                yield LogEvent(
                    timestamp = row.get('timestamp', ''),
                    level = row.get('level', ''),
                    source = row.get('source', ''),
                    message = row.get('message', '')
                )
            except Exception as e:
                if not isinstance(e, InvalidEventError):
                    raise InvalidEventError(f'Ошибка в строке CSV: {e}') from e
                raise

@register_parser('jsonl')
@register_parser('json')
def parse_jsonl_stream(file_path: Path) -> Iterator[LogEvent]:
    with open(file_path, 'r', encoding = 'utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                yield LogEvent(
                    timestamp = data.get('timestamp', ''),
                    level = data.get('level' , ''),
                    source = data.get('source', ''),
                    message = data.get('message', '')
                )
            except json.JSONDecodeError as e:
                raise InvalidEventError(f'Ошибка декодирования JSON в строке {line_num}: {e}')
            except Exception as e:
                if not isinstance(e, InvalidEventError):
                    raise InvalidEventError(f'Ошибка в строке {line_num} JSONL: {e}')
                raise

def get_parser(file_path: Path, format_override: str | None = None) -> Iterator[LogEvent]:
    if format_override:
        fmt = format_override.lower()
    else:
        fmt = file_path.suffix.lstrip('.').lower()

    if fmt not in PARSER_REGISTRY:
        raise UnsupportedFormatError(f'Неподдерживаемый формат файла или расширение: {fmt}')

    return PARSER_REGISTRY[fmt](file_path)

def tail_jsonl_stream(file_path: Path, poll_interval: float = 0.1) -> Iterator[LogEvent]:
    while not file_path.exists():
        time.sleep(poll_interval)

    with open(file_path, 'r', encoding = 'utf-8') as f:
        f.seek(0, SEEK_END) if 'os' in globals() else f.seek(0, 2)

        while True:
            line = f.readline()
            if not line:
                time.sleep(poll_interval)
                continue

            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
                yield LogEvent(
                    timestamp = data.get('timestamp', ''),
                    level = data.get('level', ''),
                    source = data.get('source', ''),
                    message = data.get('message', '')
                )
            except Exception as e:
                raise InvalidEventError(f'Ошибка в дописываемой строке: {e}') from e