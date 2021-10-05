# from openpyxl import load_workbook
import csv
import json
import sys
from pathlib import Path
from collections import defaultdict

from error import GpasError


def parse_row(d, wd=None):
    errors = []
    samples = []

    name = d["name"]

    if "Illumina" not in d["instrument_platform"]:
        errors.append({"instrument": "bad-instrument"})

    fq1_path = wd / Path(d["fastq1"])
    fq2_path = wd / Path(d["fastq2"])

    if not fq1_path.exists() or not fq2_path.exists():
        errors.append({"sample": name, "error": "file-missing"})

    return Sample(name, fq1=fq1_path, fq2=fq2_path, data=d), errors


class Sample:
    name = None
    data = None
    fq1 = None
    fq2 = None

    def __init__(self, name, fq1, fq2=None, data=None):
        if not data:
            self.data = {}
        self.name = name
        self.fq1 = fq1
        self.fq2 = fq2

    def files(self):
        if not self.fq2:
            return [str(self.fq1.name)]
        else:
            return [str(self.fq1.name), str(self.fq2.name)]

    def add_pe(self, fq1, fq1md5, fq2, fq2md5, batchname):
        self.data["peReads"] = [
            {"r1_uri": str(batchname / fq1), "r1_md5": fq1md5},
            {"r2_uri": str(batchname / fq2), "r2_md5": fq2md5},
        ]

    def add_se(self, fq, fqmd5, batchname):
        self.data["seReads"] = [{"uri": str(batchname / fq), "md5": fqmd5}]

    def to_submission(self):
        j = {
            "name": self.name,
            "fastq1": self.data["fastq1"],
            "fastq2": self.data["fastq2"],
            "specimenOrganism": self.data["specimenOrganism"],
            "host": self.data["host"],
            "collectionDate": self.data["collectionDate"],
            "country": self.data["country"],
            "submissionTitle": self.data["submissionTitle"],
            "submissionDescription": self.data["submissionDescription"],
            "status": "Uploaded",
            "instrument": {
                "platform": self.data["instrument_platform"],
                "model": self.data["instrument_model"],
                "flowcell": self.data["flowcell"],
            },
        }

        if "peReads" in self.data:
            j["peReads"] = self.data["peReads"]
        elif "seReads" in self.data:
            j["seReads"] = self.data["seReads"]
        return j


class Samplesheet:
    samples = []
    batch = None
    parent = None
    errors = []

    def __init__(self, fn, fastq_prefix=None):
        with open(fn, "r") as fd:
            reader = csv.DictReader(fd)
            for index, row in enumerate(reader):
                rowdata, rowerror = parse_row(row, wd=fn.parent)
                if not rowerror:
                    self.samples.append(rowdata)
                else:
                    self.errors.append(rowerror)

            self.batch = fn.stem

    def validate(self):
        if not self.errors:
            samples = []
            for sample in self.samples:
                samples.append({"sample": sample.name, "files": sample.files()})

            return {"validation": {"status": "completed", "samples": samples}}
        else:
            return {"validation": {"status": "failure", "samples": self.errors}}

    def make_submission(self, samples):
        good_samples = []
        for sample in samples:
            good_samples.add(
                {"pe_reads": add_reads(fq1, fq1md5, fq2, fq2md5, self.batch)}
            )
        return {"submission": batch(self.batchname, good_samples)}
