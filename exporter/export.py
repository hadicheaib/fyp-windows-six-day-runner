"""Publish one Excel view of the latest complete candidate CSV."""
import csv
import hashlib
import io
import os
from pathlib import Path
import re
import signal
import tempfile
from threading import Event

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

ILLEGAL_CELL_TEXT = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\ufffe\uffff]")


def publish(data, destination, fixture):
    rows = csv.reader(io.StringIO(data.decode("utf-8-sig"), newline=""))
    columns = next(rows)
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet("Fixture observations" if fixture else "Website candidates")
    sheet.freeze_panes = "A2"
    header = []
    for column, name in enumerate(columns, 1):
        cell = WriteOnlyCell(sheet, value=name.replace("_", " ").title())
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="214D3D")
        header.append(cell)
        sheet.column_dimensions[get_column_letter(column)].width = {
            "website": 54, "entity": 32, "domain": 27, "site_key": 32,
        }.get(name, 21)
    sheet.append(header)
    last_row = 1
    for last_row, values in enumerate(rows, 2):
        if len(values) != len(columns):
            raise ValueError("Candidate CSV has an unexpected row width")
        cells = []
        for value in values:
            cell = WriteOnlyCell(sheet, value=ILLEGAL_CELL_TEXT.sub("", value)[:32767])
            cell.data_type = "s"
            cells.append(cell)
        sheet.append(cells)
    sheet.auto_filter.ref = f"A1:{get_column_letter(len(columns))}{last_row}"
    with tempfile.NamedTemporaryFile(dir=destination.parent, suffix=".xlsx", delete=False) as file:
        temporary = Path(file.name)
    try:
        workbook.save(temporary)
        os.replace(temporary, destination)
    finally:
        workbook.close()
        temporary.unlink(missing_ok=True)


def main():
    mode = os.environ["FYP_RUN_MODE"]
    source = Path("/data") / mode / "exports/candidate-observations.csv"
    name = "Fixture_Website_Observations.xlsx" if mode == "fixture" else "Website_Observations.xlsx"
    destination = Path("/exports") / name
    stopped = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda signum, frame: stopped.set())
    previous = None
    while not stopped.is_set():
        try:
            data = source.read_bytes()
            digest = hashlib.sha256(data).digest()
            if digest != previous or not destination.exists():
                publish(data, destination, mode == "fixture")
                previous = digest
                print("Published candidate workbook", flush=True)
        except FileNotFoundError:
            pass  # The collector has yet to publish an export.
        except OSError as error:
            print(f"Export pending ({type(error).__name__})", flush=True)
        stopped.wait(10)


if __name__ == "__main__":
    main()
