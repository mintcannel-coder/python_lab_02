import argparse
import sys
from pathlib import Path

from .analysis import LogAnalyzer, filter_by_level, filter_by_source, filter_by_time
from .errors import CLIConfigurationError, StreamStatsError
from .models import CLIConfiguration
from .parsers import ParseError, get_parser
from .report import save_csv_report, save_json_report


def main():
    parser = argparse.ArgumentParser(description = 'Потоковый анализатор логов (CSV / JSONL)')
    subparsers = parser.add_subparsers(dest = 'command', required = True)

    main_parser = subparsers.add_parser('analyze', help = 'Проанализировать файлы логов')
    main_parser.add_argument('input', nargs = '+', type = Path, help = 'Пути к входным файлам')
    main_parser.add_argument('--format', type = str, choices = ['jsonl', 'csv', 'bin'], help = 'Формат входных файлов', required = True)
    main_parser.add_argument('--skip-invalid', action = 'store_true', help = 'Пропускать невалидные строки вместо падения')
    main_parser.add_argument('--output', type = Path, default = Path('report.json'), help = 'Путь к результирующему отчету')
    main_parser.add_argument('--level', nargs = '+', help = 'Фильтровать по уровням')
    main_parser.add_argument('--source', nargs = '+', help = 'Фильтровать по источникам')
    main_parser.add_argument('--start-time', help = 'Начальное время (ISO 8601)')
    main_parser.add_argument('--end-time', help = 'Конечное время (ISO 8601)')

    args = parser.parse_args()

    try:
        config = CLIConfiguration(
            output = args.output,
            format = args.format
        )

        if args.command == 'analyze':
            analyzer = LogAnalyzer()

            for file_path in args.input:
                if not file_path.exists():
                    print(f'Ошибка: файл {file_path} не найден.', file = sys.stderr)
                    sys.exit(1)

                try:
                    event_stream = get_parser(file_path, config.format)
                    if args.level:
                        event_stream = filter_by_level(event_stream, set(args.level))
                    if args.source:
                        event_stream = filter_by_source(event_stream, set(args.source))
                    if args.start_time or args.end_time:
                        event_stream = filter_by_time(event_stream, args.start_time, args.end_time)
                    for event in event_stream:
                        if isinstance(event, ParseError):
                            if args.skip_invalid:
                                print( f'WARNING: Пропущена строка №{event.line_num} в файле {file_path}: {event.err}', file=sys.stderr)
                                analyzer.skipped_lines += 1
                            else:
                                print(f'ERROR: Ошибка в файле {file_path} на строке №{event.line_num}: {event.err}', file=sys.stderr)
                                sys.exit(2)
                        else:
                            analyzer.add_event(event)
                except StreamStatsError as e:
                    print(f'Ошибка в файле {file_path}: {e}', file=sys.stderr)
                    sys.exit(2)

            report_data = analyzer.to_dict()

            if config.output.suffix.lower() == '.csv':
                save_csv_report(report_data, config.output)
            else:
                save_json_report(report_data, config.output)

            print(f'Отчет успешно сохранен в {config.output}')
    except CLIConfigurationError as e:
        print(f'Ошибка конфигурации CLI: {e}', file = sys.stderr)
        sys.exit(2)

if __name__ == '__main__':
    main()