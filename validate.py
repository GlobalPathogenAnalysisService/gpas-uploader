# from openpyxl import load_workbook
import csv
import json
from pathlib import Path
from collections import defaultdict

from error import GpasError


def parse_row(d):
    print(d)
    result, e = {}, None
    if "Illumina" not in d["instrument_platform"]:
        e.append({"instrument": "bad-instrument"})

    return d, e


class Samplesheet:
    samples = defaultdict(list)
    root = None

    def __init__(self, fn, fastq_prefix=None):
        with open(fn, "r") as fd:
            self.root = fn.root
            reader = csv.DictReader(fd)
            errors = {}  # list of parsing errors indexed by row

            for index, row in enumerate(reader):
                try:
                    rowdata, rowerror = parse_row(row)
                    if not errors:
                        self.samples[index].append(rowdata)
                    else:
                        errors[index] = rowerror
                except:
                    # propagate parsing errors
                    raise GpasError(errors)

            for index in self.samples:
                fq_path = Path(self.samples[index]["sample_filename"])
                missing_files = []
                if not fq_path.exists():
                    missing_files.append((index, fq_path))

                if missing_files:
                    raise GpasError({"row": index, "error": "file-missing"})

    def to_json(self):
        return json.dumps(self.samples)
