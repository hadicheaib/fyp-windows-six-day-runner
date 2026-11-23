"""Single-writer portable XLSX publisher for the Linux deployment.

The private Artifact Tool runtime is unavailable on Octopus. openpyxl is a
deployment dependency here; local deliverable inspection uses Artifact Tool.
"""
from __future__ import annotations

import csv
import os
from pathlib import Path
import re


def text(value):
    return re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', str(value))[:32767]


def publish(folder: Path, payload: dict):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.worksheet.table import Table, TableStyleInfo
    from openpyxl.utils import get_column_letter

    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    book = Workbook()
    progress = book.active
    progress.title = 'Progress'
    links = book.create_sheet('Links')
    navy, pale = '24364B', 'EEF2F5'

    def put(sheet, row, values):
        for column, value in enumerate(values, 1):
            cell = sheet.cell(row, column)
            if isinstance(value, str):
                cell.value = text(value)
                cell.data_type = 's'  # Provider labels never become Excel formulas.
            else:
                cell.value = value
            cell.font = Font(name='Calibri', size=11, color='263548')
            cell.alignment = Alignment(vertical='center', wrap_text=True)

    def header(sheet, row, labels):
        put(sheet, row, labels)
        sheet.row_dimensions[row].height = 34
        for cell in sheet[row][:len(labels)]:
            cell.fill = PatternFill('solid', fgColor=navy)
            cell.font = Font(name='Calibri', size=11, bold=True, color='FFFFFF')

    fixture = payload['mode'] == 'fixture'
    put(progress, 1, ['HPC map-walk test' if fixture else 'HPC website discovery'])
    progress.merge_cells('A1:J1')
    progress['A1'].font = Font(name='Calibri', size=19, bold=True, color=navy)
    progress.row_dimensions[1].height = 34
    put(progress, 2, ['Synthetic pages only. These links are test data.' if fixture else
                     'Provider observations await website/entity/Lebanon review.'])
    progress.merge_cells('A2:J2')
    progress.row_dimensions[2].height = 28
    summaries = [
        ['Run status', payload['pool_status'], None, 'Stop reason', None, payload.get('reason') or 'none'],
        ['Elapsed seconds', payload.get('elapsed_seconds', 0), None, 'Requested seconds', None, payload['requested_seconds']],
        ['New link observations', payload['links'], None, 'All saved link observations', None, len(payload['all_links'])],
        ['Reviews completed', 0, None, 'Supported domain gains', None, 0],
        ['Production imports', 0],
    ]
    for row_number, values in enumerate(summaries, 4):
        put(progress, row_number, values)
        progress.row_dimensions[row_number].height = 23
        if len(values) > 2:
            progress.merge_cells(f'D{row_number}:E{row_number}')
    header(progress, 9, ['Governorate', 'Worker status', 'New links', 'No website', 'Unresolved',
                         'Views inspected this run', 'Pending views', 'Quarantined views', 'Map grants', 'Click grants'])
    for row, region in enumerate(payload['regions'], 10):
        states = region['view_states']
        put(progress, row, [region['region'], region['status'], region['links'], region['no_website'],
             region['unresolved_attempts'], region.get('views_inspected', 0),
             states.get('pending', 0) + states.get('paused', 0), states.get('unresolved', 0),
             region.get('maps', 0), region.get('clicks', 0)])
        progress.row_dimensions[row].height = 30
        if row % 2 == 0:
            for cell in progress[row][:10]:
                cell.fill = PatternFill('solid', fgColor=pale)
    put(progress, 21, ['Website availability is not checked. Inspected views do not establish exhaustive coverage.'])
    progress.merge_cells('A21:J21')
    progress.row_dimensions[21].height = 30
    for column, width in enumerate([25, 25, 13, 14, 13, 18, 15, 18, 13, 13], 1):
        progress.column_dimensions[get_column_letter(column)].width = width
    progress.freeze_panes = 'C10'
    progress.sheet_view.showGridLines = False

    labels = ['Governorate', 'Entity', 'Website URL', 'Site key', 'Registrable domain',
              'Review', 'HTTP availability', 'Source identity', 'Observed time (Beirut)', 'New in this run']
    header(links, 1, labels)
    for row_number, row in enumerate(payload['all_links'], 2):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        observed = datetime.fromtimestamp(row['observed_unix'], ZoneInfo('Asia/Beirut')).isoformat(timespec='seconds')
        put(links, row_number, [row['region'], row.get('label', ''), row['website'], row.get('site_key', ''),
             row.get('domain', ''), row.get('review', 'pending'), row.get('http_availability', 'not_checked'),
             row.get('identity', ''), observed, row['this_run']])
        links.row_dimensions[row_number].height = 36
    if payload['all_links']:
        table = Table(displayName='ObservedLinks', ref=f'A1:J{len(payload["all_links"]) + 1}')
        table.tableStyleInfo = TableStyleInfo(name='TableStyleMedium2', showRowStripes=True)
        links.add_table(table)
    for column, width in enumerate([25, 36, 57, 38, 30, 15, 20, 42, 29, 19], 1):
        links.column_dimensions[get_column_letter(column)].width = width
    links.freeze_panes = 'C2'
    links.sheet_view.showGridLines = False
    temp = folder / 'collected-links.writing.xlsx'
    book.save(temp)
    book.close()
    with temp.open('r+b') as file:
        os.fsync(file.fileno())
    os.replace(temp, folder / 'collected-links.xlsx')

    # CSV and JSON are companion views; SQLite remains authoritative.
    csv_path = folder / 'collected-links.writing.csv'
    with csv_path.open('w', encoding='utf-8-sig', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=labels)
        writer.writeheader()
        for row in links.iter_rows(min_row=2, values_only=True):
            writer.writerow({label: ("'" + value if isinstance(value, str) and value.startswith(('=', '+', '-', '@')) else value)
                             for label, value in zip(labels, row)})
        file.flush()
        os.fsync(file.fileno())
    os.replace(csv_path, folder / 'collected-links.csv')
