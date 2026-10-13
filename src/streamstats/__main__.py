import argparse
import sys
from pathlib import Path

from .analysis import LogAnalyzer, filter_by_level, filter_by_source, filter_by_time
from .errors import StreamStatsError
from .parsers import get_parser
from .report import save_csv_report, save_report


def main():
    parser = argparse.ArgumentParser(description = 'Потоковый анализатор логов (CSV / JSONL)')
    subparsers = parser.add_subparsers(dest = 'command', required = True)

    analyze_parser = subparsers.add_parser('analyze', help = 'Проанализировать файлы логов')
    analyze_parser.add_argument('inputs', nargs = '+', type = Path, help = 'Пути к входным файлам (CSV или JSONL)')
    analyze_parser.add_argument('--skip-invalid', action = 'store_true', help = 'Пропускать невалидные строки вместо падения')
    analyze_parser.add_argument('output', type = Path, help = 'Путь к результирующему JSON-отчету')
    analyze_parser.add_argument('--level', nargs = '+', help = 'Фильтровать по уровням (например, ERROR CRITICAL)')
    analyze_parser.add_argument('--source', nargs = '+', help = 'Фильтровать по источникам')
    analyze_parser.add_argument('--start-time', help = 'Начальное время (ISO 8601)')
    analyze_parser.add_argument('--end-time', help = 'Конечное время (ISO 8601)')

    args = parser.parse_args()

    if args.command == 'analyze':
        analyzer = LogAnalyzer()

        for file_path in args.inputs:
            if not file_path.exists():
                print(f'Ошибка: файл {file_path} не найден.', file = sys.stderr)
                sys.exit(1)

            try:
                event_stream = get_parser(file_path)
                if args.level:
                    event_stream = filter_by_level(event_stream, set(args.level))
                if args.source:
                    event_stream = filter_by_source(event_stream, set(args.source))
                if args.start_time or args.end_time:
                    event_stream = filter_by_time(event_stream, args.start_time, args.end_time)
                line_idx = 0
                while True:
                    try:
                        event = next(event_stream, None)
                        if event is None:
                            break
                        line_idx +=  1
                        analyzer.add_event(event)
                    except StreamStatsError as e:
                        line_idx +=  1
                        if args.skip_invalid:
                            analyzer.skipped_lines +=  1
                            print(f'WARNING: Пропущена строка в файле {file_path} (строка ~{line_idx}): {e}', file = sys.stderr)
                        else:
                            print(f'ERROR: Ошибка в файле {file_path} на строке ~{line_idx}: {e}', file = sys.stderr)
                            sys.exit(2)

            except StreamStatsError as e:
                print(f'Ошибка при обработке файла {file_path}: {e}', file = sys.stderr)
                sys.exit(2)

        report_data = analyzer.to_dict()

        output_ext = args.output.suffix.lower()

        if output_ext == '.csv':
            save_csv_report(report_data, args.output)
        else:
            save_report(report_data, args.output)

        print(f'Отчет успешно сохранен в {args.output}')

if __name__ == '__main__':
    main()