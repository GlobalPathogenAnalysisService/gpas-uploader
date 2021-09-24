# from openpyxl import load_workbook
import csv
import json
import sys
from pathlib import Path
from collections import defaultdict

from error import GpasError


def parse_row(d):
    result, e = {}, []
    if "Illumina" not in d["instrument_platform"]:
        e.append({"instrument": "bad-instrument"})

    fq1_path = Path(d["fastq1"])
    fq2_path = Path(d["fastq2"])

    if not fq1_path.exists() or not fq2_path.exists():
        e.append({"sample": 0, "error": "file-missing"})
        # raise GpasError({"row": index, "error": "file-missing"})

    print(d, e, file=sys.stderr)
    return d, e


class Samplesheet:
    samples = []
    batch = None
    root = None

    def __init__(self, fn, fastq_prefix=None):
        with open(fn, "r") as fd:
            self.root = fn.root
            reader = csv.DictReader(fd)
            errors = {}  # list of parsing errors indexed by row

            for index, row in enumerate(reader):
                #                try:
                rowdata, rowerror = parse_row(row)
                if not errors:
                    self.samples.append(rowdata)
                else:
                    errors[index] = rowerror
            #                except:
            # propagate parsing errors
            #    raise GpasError(errors)

            missing_files = []
        self.batch = str(fn.name)

    def to_json(self):
        return {"batch": self.batch, "samples": self.samples}
