import csv
import json
import struct
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from os import SEEK_END
from pathlib import Path

from .errors import (
    InvalidEventError,
    StreamStatsError,
    UnsupportedFormatError,
)
from .models import LogEvent

PARSER_REGISTRY: dict[str, Callable[[Path], Iterator[LogEvent]]] = {}

@dataclass
class ParseError:
    line_num: int
    err: Exception

def register_parser(format_name: str):
    def decorator(func: Callable[[Path], Iterator[LogEvent]]):
        PARSER_REGISTRY[format_name.lower()] = func
        return func
    return decorator


def get_parser(file_path: Path, format_override: str | None = None) -> Iterator[LogEvent]:
    if format_override:
        fmt = format_override.lower()
    else:
        fmt = file_path.suffix.lstrip('.').lower()

    if fmt not in PARSER_REGISTRY:
        raise UnsupportedFormatError(f'Неподдерживаемый формат файла или расширение: {fmt}')

    return PARSER_REGISTRY[fmt](file_path)

@register_parser('csv')
def parse_csv_stream(file_path: Path) -> Iterator[LogEvent | ParseError]:
    with open(file_path, 'r', encoding = 'utf-8', newline = '') as f:
        reader = csv.DictReader(f)
        for row in reader:
            line_num = reader.line_num
            try:
                yield LogEvent(
                    timestamp = row.get('timestamp', ''),
                    level = row.get('level', ''),
                    source = row.get('source', ''),
                    message = row.get('message', '')
                )
            except StreamStatsError as e:
                yield ParseError(line_num, e)
                continue
                    

@register_parser('jsonl')
@register_parser('json')
def parse_jsonl_stream(file_path: Path) -> Iterator[LogEvent | ParseError]:
    with open(file_path, 'r', encoding = 'utf-8') as f:
        for line_num, line in enumerate(f, 1):
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

            except (json.JSONDecodeError, StreamStatsError, AttributeError) as e:
                yield ParseError(line_num, e)
                continue

@register_parser('bin')
def parse_binary_stream(file_path: Path) -> Iterator[LogEvent]:
    with open(file_path, 'rb') as f:
        while True:
            pkg_bin = f.read(4)
            if not pkg_bin:
                break
            if len(pkg_bin) < 4:
                raise InvalidEventError('Ошибка чтения заголовка бинарного пакета.') 
            pkg_len = struct.unpack('>I', pkg_bin)[0]

            packet = bytearray(pkg_len)
            mv = memoryview(packet)

            offset = 0
            while offset < pkg_len:
                bytes_read = f.readinto(mv[offset:])

                if not bytes_read:
                    raise InvalidEventError('Ошибка чтения бинарного пакета: тело пакета короче заявленной длины')
                offset += bytes_read

            try:
                data = json.loads(packet)

                if not isinstance(data, dict):
                    raise InvalidEventError('Бинарный пакет должен содержать JSON-объект')

                yield LogEvent(
                    timestamp = data.get('timestamp', ''),
                    level = data.get('level', ''),
                    source = data.get('source', ''),
                    message = data.get('message', '')
                )
            except StreamStatsError:
                raise
            except (UnicodeDecodeError, json.JSONDecodeError, AttributeError) as e:
                raise InvalidEventError(f'Ошибка парсинга бинарного пакета: {e}') from e


def tail_jsonl_stream(file_path: Path, poll_interval: float = 0.1) -> Iterator[LogEvent]:
    while not file_path.exists():
        time.sleep(poll_interval)

    with open(file_path, 'r', encoding='utf-8') as f:
        f.seek(0, SEEK_END)
        pending = ''

        while True:
            chunk = f.readline()

            if not chunk:
                time.sleep(poll_interval)
                continue

            pending += chunk

            if not pending.endswith('\n'):
                continue

            line = pending.strip()
            pending = ''

            if not line:
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise InvalidEventError(f'Ошибка в дописываемой строке: {e}') from e

            if not isinstance(data, dict):
                raise InvalidEventError('Ошибка в дописываемой строке: ожидался JSON-объект.')

            try:
                yield LogEvent(
                    timestamp=data.get('timestamp', ''),
                    level=data.get('level', ''),
                    source=data.get('source', ''),
                    message=data.get('message', ''),
                )
            except StreamStatsError as e:
                raise InvalidEventError(f'Ошибка в дописываемой строке: {e}') from e