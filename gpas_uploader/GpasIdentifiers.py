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



def assign_gpas_identifiers_oci(row, run_lookup, guid_lookup):
    if pandas.isna(row.run_number) or row.run_number == '':
        gpas_run_number = ""
    else:
        gpas_run_number = run_lookup[row.run_number]

    if 'r_md5' in row.keys():
        gpas_sample_name = guid_lookup[row['r_md5']]
    else:
        gpas_sample_name = guid_lookup[row['r1_md5']]

    return pandas.Series([gpas_sample_name, gpas_run_number])


def assign_gpas_identifiers_local(row, lookup):

    gpas_sample_name = str(uuid.uuid4())
    if row.run_number == '':
        gpas_run_number = ''
    else:
        gpas_run_number = lookup[row.run_number]

    return pandas.Series([gpas_sample_name, gpas_run_number])
