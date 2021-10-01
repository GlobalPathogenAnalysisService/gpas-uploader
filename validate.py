# from openpyxl import load_workbook
import csv
import json
import sys
from pathlib import Path
from collections import defaultdict

from error import GpasError


def parse_row(d):
    errors = []
    if "Illumina" not in d["instrument_platform"]:
        errors.append({"instrument": "bad-instrument"})

    fq1_path = Path(d["fastq1"])
    fq2_path = Path(d["fastq2"])

    if not fq1_path.exists() or not fq2_path.exists():
        errors.append({"sample": 0, "error": "file-missing"})
        # raise GpasError({"row": index, "error": "file-missing"})

    # print(d, e, file=sys.stderr)

    return (
        {
            "name": d["name"],
            "fastq1": d["fastq1"],
            "fastq2": d["fastq2"],
            "specimenOrganism": d["specimenOrganism"],
            "host": d["host"],
            "collectionDate": d["collectionDate"],
            "country": d["country"],
            "submissionTitle": d["submissionTitle"],
            "submissionDescription": d["submissionDescription"],
            "status": "Uploaded",
            "instrument": {
                "platform": d["instrument_platform"],
                "model": d["instrument_model"],
                "flowcell": d["flowcell"],
            },
        },
        errors,
    )


def add_reads(fq1, fq1md5, fq2, fq2md5, batchname):
    return ({"uri": batchname / fq1.name, "md5": fq1md5},)


def batch(batchname, samples):
    return {
        "batch": {
            "fileName": batchname,
            "uploadedBy": "test",
            "organisation": "test",
            "site": "test",
            "uploadedOn": "2021-09-27T22:27:49.304Z",
            "samples": samples,
        }
    }


class Samplesheet:
    samples = []
    batch = None
    parent = None

    def __init__(self, fn, fastq_prefix=None):
        with open(fn, "r") as fd:
            reader = csv.DictReader(fd)
            errors = {}  # list of parsing errors indexed by row
            samples = []
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

            self.batch = fn.stem

    def make_submission(self, samples):
        good_samples = []
        for sample in samples:
            good_samples.add(
                {"pe_reads": add_reads(fq1, fq1md5, fq2, fq2md5, self.batch)}
            )
        return {"submission": batch(self.batchname, good_samples)}
