import argparse
import pathlib
import datetime
import pkg_resources
import copy

import pandas
import pandera

import gpas_uploader

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--upload_csv", required=True, help="path to the metadata CSV file that will be passed to the GPAS upload client")
    parser.add_argument("--tag_file", required=False, default=pkg_resources.resource_filename("gpas_uploader", 'data/tags.txt'),help="a plaintext file containing the allowed GPAS tags for this upload user -- the list is available on the GPAS portal under Upload | Tags. If not specified, then a default set is used.")
    options = parser.parse_args()

    # read in the specified upload CSV
    upload_csv = pathlib.Path(options.upload_csv)
    assert upload_csv.is_file(), 'provided upload CSV does not exist!'
    df = pandas.read_csv(upload_csv, dtype=object)
    assert 'sample_name' in df.columns, 'upload CSV must contain a column called sample_name that contains the sample names'
    df.set_index('sample_name', inplace=True,verify_integrity=False)

    def format_error(row):
        return("problem in "+ row.column)

    # be ready for errors!
    checks_pass = True
    errors = pandas.DataFrame(None, columns=['sample_name', 'error_message'])

    def build_errors(err, errors):
        failures = err.failure_cases
        failures.rename(columns={'index':'sample'}, inplace=True)
        failures['error'] = failures.apply(format_error, axis=1)
        errors = errors.append(failures[['sample_name', 'error_message']])
        return(errors, False)

    df['gpas_sample_name'] = 'a'
    df['gpas_batch'] = 'a'
    df['gpas_run_number'] = 1
    print(df)

    if 'fastq1' in df.columns:
        try:
            gpas_uploader.IlluminaFASTQCheckSchema.validate(df, lazy=True)
        except pandera.errors.SchemaErrors as err:
            errors, checks_pass = build_errors(err, errors)
    elif 'fastq' in df.columns:
        try:
            gpas_uploader.NanoporeFASTQCheckSchema.validate(df, lazy=True)
        except pandera.errors.SchemaErrors as err:
            errors, checks_pass = build_errors(err, errors)
    elif 'bam' in df.columns:
        try:
            gpas_uploader.BAMFASTQCheckSchema.validate(df, lazy=True)
        except pandera.errors.SchemaErrors as err:
            errors, checks_pass = build_errors(err, errors)

    # check against tags
    tag_file = pathlib.Path(options.tag_file)
    assert tag_file.is_file(), 'provided tag file does not exist!'

    allowed_tags = set()
    with open(tag_file, 'r') as INPUT:
        for line in INPUT:
            allowed_tags.add(line.rstrip())

    bad_tags = set()
    def check_tags(row):
        tags_ok = True
        cols = row['tags'].split(':')
        for i in cols:
            if i not in allowed_tags:
                tags_ok = False
                bad_tags.add(i)
        return tags_ok

    df['tags_ok'] = df.apply(check_tags,axis=1)

    if all(df.tags_ok):
        print("All tags validate against the tags provided in " + options.tag_file )

    else:
        checks_pass=False
        failures = copy.deepcopy(df[~df.tags_ok])
        failures.reset_index(inplace=True)
        failures.rename(columns={'name': 'sample_name'}, inplace=True)
        failures['error'] = 'tags do not validate'
        errors = errors.append(failures[['sample', 'error']])

    print()
    if checks_pass:
        print("--> All preliminary checks pass and this upload CSV can be passed to the GPAS upload client")
    else:
        print(errors[['sample', 'error']])
        print()
        print("--> Please fix the above errors and try validating again. Do not pass this upload CSV to the GPAS upload client.")
