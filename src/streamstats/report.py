import csv
import json
from pathlib import Path


def save_json_report(report_data: dict, output_path: Path):
    with open(output_path, 'w', encoding = 'utf-8') as f:
        json.dump(report_data, f, ensure_ascii = False, indent = 2)

def save_csv_report(report_data: dict, output_path: Path):
    with open(output_path, 'w', encoding = 'utf-8', newline = '') as f:
        writer = csv.writer(f)
        writer.writerow(['Metric', 'Value'])
        writer.writerow(['total_events', report_data.get('total_events', 0)])
        writer.writerow(['skipped_lines', report_data.get('skipped_lines', 0)])
        writer.writerow(['first_timestamp', report_data.get('first_timestamp', '')])
        writer.writerow(['last_timestamp', report_data.get('last_timestamp', '')])
        writer.writerow([])

        writer.writerow(['Level', 'Count'])
        for level, count in report_data.get('levels', {}).items():
            writer.writerow([level, count])

        writer.writerow([])
        writer.writerow(['Source', 'Count'])

        for source, count in report_data.get('sources', {}).items():
            writer.writerow([source, count])
        writer.writerow([])

        writer.writerow(['Top Error Source', 'Count'])
        for source, count in report_data.get('top_error_sources', {}).items():
            writer.writerow([source, count])