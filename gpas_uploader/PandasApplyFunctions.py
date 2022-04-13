#! /usr/bin/env python3

import hashlib
from pathlib import Path
import requests

import pandas

import gpas_uploader

def hash_paired_reads(row, wd):
    """Calculate the MD5 and SHA hashes for two FASTQ files containing paired reads.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row: pandas.Series
        supplied by pandas.DataFrame.apply
    wd: pathlib.Path
        the working directory

    Returns
    -------
    pandas.Series
    """
    fq1md5, fq1sha = hash_fastq(row.r1_uri)
    fq2md5, fq2sha = hash_fastq(row.r2_uri)
    return(pandas.Series([fq1md5, fq1sha, fq2md5, fq2sha]))


def hash_unpaired_reads(row, wd):
    """Calculate the MD5 and SHA hashes for a FASTQ file containing unpaired reads.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row: pandas.Series supplied by pandas.DataFrame.apply
    wd: pathlib.Path of the working directory

    Returns
    -------
    pandas.Series
    """
    fqmd5, fqsha = hash_fastq(row.r_uri)
    return(pandas.Series([fqmd5, fqsha]))


def hash_fastq(filename):
    """Calculate the MD5 and SHA hashes of a file.

    Parameters
    ---------
    filename: pathlib.Path
        the file to hash

    Returns
    -------
    str
        MD5 hash of FASTQ file
    str
        SHA hash of FASTQ file
    """
    md5 = hashlib.md5()
    sha = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
            sha.update(chunk)
    return md5.hexdigest(), sha.hexdigest()


def build_errors(err):
    """Parse the errors returned from the panderas class.

    Parameters
    ----------
    err: pandera.errors.SchemaErrors

    Returns
    -------
    pandas.DataFrame(columns=['sample_name', 'error_message'])
    """
    failures = err.failure_cases
    failures.rename(columns={'index':'sample_name'}, inplace=True)
    failures['error_message'] = failures.apply(format_error, axis=1)
    return(failures[['sample_name', 'error_message']])


def format_error(row):
    """Mong the panderas errors into more user-friendly error messages

    Returns
    -------
    str
        error message, defaults to 'problem in <field_name> field'
    """

    if row.check == 'column_in_schema':
        return('unexpected column ' + row.failure_case + ' found in upload CSV')
    if row.check == 'column_in_dataframe':
        return('column ' + row.failure_case + ' missing from upload CSV')
    elif row.check == 'region_is_valid':
        return("specified regions are not valid ISO-3166-2 regions for the specified country")
    elif row.check == 'instrument_is_valid':
        return 'FASTQ file columns and instrument_platform are inconsistent'
    elif row.check == 'not_nullable':
        return row.column + ' cannot be empty'
    elif row.check == 'field_uniqueness':
        return row.column + ' must be unique in the upload CSV'
    elif 'str_matches' in row.check:
        allowed_chars = row.check.split('[')[1].split(']')[0]
        if row.schema_context == 'Column':
            return(row.column + ' can only contain characters (' + allowed_chars + ')')
        elif row.schema_context == 'Index':
            return('sample_name can only contain characters (' + allowed_chars + ')')
    elif row.column == 'country' and row.check[:4] == 'isin':
        return(row.failure_case + " is not a valid ISO-3166-1 country")
    elif row.column == 'region' and row.check[:4] == 'isin':
        return(row.failure_case + " is not a valid ISO-3166-2 region")
    elif row.column == 'control' and row.check[:4] == 'isin':
        return(row.failure_case + ' in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative')
    elif row.column == 'host' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword human')
    elif row.column == 'specimen_organism' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword SARS-CoV-2')
    elif row.column == 'primer_scheme' and row.check[:4] == 'isin':
        return(row.column + ' can only contain the keyword auto')
    elif row.column == 'instrument_platform':
        if row.sample_name is None:
            return(row.column + ' must be unique')
        if row.check[:4] == 'isin':
            return(row.column + ' can only contain one of the keywords Illumina or Nanopore')
    elif row.column == 'collection_date':
        if row.sample_name is None:
            return(row.column + ' must be in form YYYY-MM-DD and cannot include the time')
        if row.check[:4] == 'less':
            return(row.column + ' cannot be in the future')
        if row.check[:7] == 'greater':
            return(row.column + ' cannot be before 2019-01-01')
    elif row.column in ['fastq1', 'fastq2', 'fastq']:
        if row.check == 'field_uniqueness':
            return(row.column + ' must be unique in the upload CSV')
    elif row.column is None:
        return("problem")
    else:
        return("problem in "+ row.column + ' field')

def rename_unpaired_fastq(row):
    """Rename the unpaired FASTQ file with the GPAS sample name

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    str
        path to the renamed FASTQ file
    """

    p = Path(row.r_uri)
    dest_file = Path(row.gpas_sample_name + '.reads.fastq.gz')
    p.rename(p.parent / dest_file)

    # PWF Sprint 11 hack to push JSON decontamination block later to help EC
    gpas_uploader.dmsg(row.gpas_sample_name, "started", msg={"file": str(p.name)}, json=True)
    gpas_uploader.dmsg(row.gpas_sample_name, "completed", msg={"file": str(p.name), "cleaned": str(p.parent / dest_file)}, json=True)

    return str(p.parent / dest_file)

def rename_paired_fastq(row):
    """Rename the paired FASTQ files with the GPAS sample name

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    pandas.Series
        paths to both renamed FASTQ files
    """

    p1, p2 = Path(row.r1_uri), Path(row.r2_uri)
    dest_file1 = Path(row.gpas_sample_name + '.reads_1.fastq.gz')
    dest_file2 = Path(row.gpas_sample_name + '.reads_2.fastq.gz')
    p1.rename(p1.parent / dest_file1)
    p2.rename(p2.parent / dest_file2)

    # PWF Sprint 11 hack to push JSON decontamination block later to help EC
    gpas_uploader.dmsg(row.gpas_sample_name, "started", msg={"file": str(p1.name)}, json=True)
    gpas_uploader.dmsg(row.gpas_sample_name, "started", msg={"file": str(p2.name)}, json=True)
    gpas_uploader.dmsg(row.gpas_sample_name, "completed", msg={"file": str(p1.name), "cleaned": str(p1.parent / dest_file1)}, json=True)
    gpas_uploader.dmsg(row.gpas_sample_name, "completed", msg={"file": str(p2.name), "cleaned": str(p2.parent / dest_file2)}, json=True)

    return pandas.Series([str(p1.parent / dest_file1), str(p2.parent / dest_file2),])

def upload_fastq_paired(row, url, headers):
    """Upload a pair of FASTQ files to the Organisation's input bucket in OCI.

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    True if upload successful (or previously done), False otherwise
    """

    if not row.uploaded:
        r1 = requests.put(url + row.name + '.reads_1.fastq.gz', open(row['r1_uri'], 'rb'), headers=headers)
        r2 = requests.put(url+ row.name + '.reads_2.fastq.gz', open(row['r2_uri'], 'rb'), headers=headers)
        return (r1.ok and r2.ok)
    else:
        return True

def upload_fastq_unpaired(row, url, headers):
    """Upload an unpaired FASTQ file to the Organisation's input bucket in OCI.

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    True if upload successful (or previously done), False otherwise
    """

    if not row.uploaded:
        r = requests.put(url + row.name + '.reads.fastq.gz', open(row['r_uri'], 'rb'), headers=headers)
        return r.ok
    else:
        return True


def check_files_exist(row, file_extension, wd):
    """"Check if a genetic file exists.

    Designed to be used with pandas.DataFrame.apply

    Parameters
    ----------
    row : pandas.Series
    file_extension: str
        the type of genetic file, one of fastq1, fastq2 or bam
    wd: pathlib.Path
        the working directory

    Returns
    -------
    None or error message if file does not exist
    """
    if isinstance(row[file_extension], str):
        if not (wd / row[file_extension]).is_file():
            return(row[file_extension] + ' does not exist')
        else:
            if (wd / row[file_extension]).stat().st_size < 100:
                return(row[file_extension] + ' is too small (< 100 bytes)')
            else:
                return(None)
    elif str(row[file_extension]) == 'nan':
        return(file_extension + ' not specified')



def check_files_exist_in_df(df, file_extension, wd):
    """Check if the genetic files specified in upload CSV exist.

    Calls check_files_exist using the pandas.DataFrame.apply method

    Parameters
    ----------
    df: pandas.DataFrame
    file_extension: str
        the type of genetic file, one of fastq1, fastq2 or bam
    wd:

    """
    df['error_message'] = df.apply(check_files_exist, args=(file_extension, wd,), axis=1)
    result = df[df.error_message.notna()]
    if result.empty:
        return(True, None)
    else:
        err = pandas.DataFrame(result.error_message, columns=['error_message'])
        err.reset_index(inplace=True)
        err.rename(columns={'name': 'sample_name'}, inplace=True)
        return(False, err)

def check_tags(row, allowed_tags):
    """Check that the tags all match a supplied set of allowed tags.

    Designed to be used with pandas.DataFrame.apply

    Returns
    -------
    bool
        True if all match, otherwise False
    """
    tags_ok = True
    cols = row['tags'].split(':')
    for i in cols:
        if i not in allowed_tags:
            tags_ok = False
    return(tags_ok)
