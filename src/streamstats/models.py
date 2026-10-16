from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .errors import CLIConfigurationError, InvalidEventError, InvalidTimestampError

VALID_LEVELS = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}

@dataclass
class LogEvent:
    timestamp: str
    level: str
    source: str
    message: str

    def __post_init__(self):
        if not isinstance(self.level, str) or self.level not in VALID_LEVELS:
            raise InvalidEventError(f'Неверный уровень лога: {self.level}')
        
        if  not isinstance(self.source, str) or not self.source or not self.source.strip():
            raise InvalidEventError('Поле source не должно быть пустым')

        if not isinstance(self.timestamp, str):
            raise InvalidTimestampError('Timestamp должен быть строкой')
        
        try:
            datetime.fromisoformat(self.timestamp)
        except ValueError as e:
            raise InvalidTimestampError(f'Неверный формат времени ISO 8601: {self.timestamp}') from e

        if not isinstance(self.message, str):
            raise InvalidEventError('Поле message должно быть строкой')

@dataclass
class CLIConfiguration:
    output: Path
    format: str

    def __post_init__(self):
        if self.output.suffix.lower() not in {'.json', '.csv'}:
            raise CLIConfigurationError(f'Неподдерживаемый формат отчета: {self.output.suffix}')

        if not self.format in {'jsonl', 'csv', 'bin'}:
            raise CLIConfigurationError(f'Неподдерживаемый формат файла: {self.format}')

        