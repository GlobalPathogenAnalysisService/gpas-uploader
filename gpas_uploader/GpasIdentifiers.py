#! /usr/bin/env python3

import hashlib
import uuid

import pandas

a = "BCDFGHJKMPQRTVWXY2346789"

def hash(fn):
    sha = hashlib.sha256()
    with open(fn, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha.update(chunk)
    return int(sha.hexdigest(), 16)


def enc(n, a=a):
    s = []

    if n == 0:
        s.append(a[0])
    while n:
        s.append(a[n % len(a)])
        n = n // len(a)

    return "".join(s)


def create_batch_name(fn):
    return enc(hash(fn))[:7]


def assign_gpas_identifiers(row, lookup):

    gpas_name = str(uuid.uuid4())
    gpas_run_number = lookup[row.run_number]

    return pandas.Series([gpas_name, gpas_run_number])
