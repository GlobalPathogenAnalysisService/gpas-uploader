# from openpyxl import load_workbook
import csv
import json
from pathlib import Path

from error import GpasError


def parse_row(d):
    result, e = {}, None
    if d["instrument"] != "Illumina":
        e.append({"instrument": "bad instrument"})

    return d, e


class Samplesheet:
    samples = {}
    root = None

    def __init__(self, fn, fastq_prefix=None):
        with open(fn) as fd:
            self.root = fn.root
            reader = csv.reader(fd, delimiter=",")
            errors = {}  # list of parsing errors indexed by row

            for index, row in enumerate(reader):
                try:
                    rowdata, errors = parse_row(row)
                    if not errors:
                        self.samples[index](rowdata)
                    else:
                        errors[index] = errors
                except:
                    # propagate parsing errors
                    raise GpasError(errors)

            for index in self.samples:
                fq_path = Path(self.samples[index]["fastq"])
                missing_files = []
                if not fq_path.exists():
                    missing_files.append((index, fq_path))

                if missing_files:
                    raise GpasError({index: "file not found"})

    def to_json(self):
        return json.dumps(self.samples)
