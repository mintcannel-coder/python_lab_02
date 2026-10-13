from dataclasses import dataclass
from datetime import datetime

from .errors import InvalidEventError, InvalidTimestampError

VALID_LEVELS = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}

@dataclass
class LogEvent:
    timestamp: str
    level: str
    source: str
    message: str

    def __post_init__(self):
        if self.level not in VALID_LEVELS:
            raise InvalidEventError(f'Неверный уровень лога: {self.level}')
        
        if not self.source or not isinstance(self.source, str) or not self.source.strip():
            raise InvalidEventError('Поле source не должно быть пустым')
            
        try:
            datetime.fromisoformat(self.timestamp)
        except ValueError as e:
            raise InvalidTimestampError(f'Неверный формат времени ISO 8601: {self.timestamp}') from e