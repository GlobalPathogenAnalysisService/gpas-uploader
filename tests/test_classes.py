import pytest, pathlib

import gpas_uploader

def test_nonASCII_upload_csv_fails():

    with pytest.raises(Exception) as e_info:
        validate.UploadBatch("tests/files/bam-nanopore-samplesheet ~ - ! 'template-good.csv")

def test_no_upload_csv_fails():

    with pytest.raises(Exception) as e_info:
        validate.UploadBatch('tests/files/none-existent-file.csv')

def test_illumina_fastq_pass_1():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-pass-1.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert a.valid

    assert a.validation_json == {
      "validation": {
        "status": "completed",
        "samples": [
          {
            "sample": "sample1",
            "files": [
              "paired1_1.fastq.gz",
              "paired1_2.fastq.gz"
            ]
          },
          {
            "sample": "sample2",
            "files": [
              "paired2_1.fastq.gz",
              "paired2_2.fastq.gz"
            ]
          },
          {
            "sample": "sample3",
            "files": [
              "paired3_1.fastq.gz",
              "paired3_2.fastq.gz"
            ]
          }
        ]
      }
    }

# check an upload CSV where
#  - the run_number is null
#  - the region is null
def test_illumina_fastq_pass_2():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-pass-2.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert a.valid

    assert a.validation_json == {
      "validation": {
        "status": "completed",
        "samples": [
          {
            "sample": "sample1",
            "files": [
              "paired1_1.fastq.gz",
              "paired1_2.fastq.gz"
            ]
          },
          {
            "sample": "sample2",
            "files": [
              "paired2_1.fastq.gz",
              "paired2_2.fastq.gz"
            ]
          },
          {
            "sample": "sample3",
            "files": [
              "paired3_1.fastq.gz",
              "paired3_2.fastq.gz"
            ]
          }
        ]
      }
    }


def test_illumina_bam_pass_1():

    a = gpas_uploader.UploadBatch('tests/files/illumina-bam-upload-csv-pass-1.csv', run_parallel=False)

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert a.valid

    assert a.validation_json == {
      "validation": {
        "status": "completed",
        "samples": [
          {
            "sample": "sample1",
            "files": [
              "paired1_1.fastq.gz",
              "paired1_2.fastq.gz"
            ]
          },
          {
            "sample": "sample2",
            "files": [
              "paired2_1.fastq.gz",
              "paired2_2.fastq.gz"
            ]
          },
          {
            "sample": "sample3",
            "files": [
              "paired3_1.fastq.gz",
              "paired3_2.fastq.gz"
            ]
          }
        ]
      }
    }

def test_nanopore_fastq_pass_1():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-pass-1.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert a.valid

    assert a.validation_json == {
      "validation": {
        "status": "completed",
        "samples": [
          {
            "sample": "sample1",
            "files": [
              "unpaired1.fastq.gz"
            ]
          }
        ]
      }
    }


def test_nanopore_bam_pass_1():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-bam-upload-csv-pass-1.csv', run_parallel=False)

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert a.valid

    assert a.validation_json == {
      "validation": {
        "status": "completed",
        "samples": [
          {
            "sample": "sample1",
            "files": [
              "unpaired1.fastq.gz"
            ]
          }
        ]
      }
    }


# valid but contains Nanopore and Illumina runs which is not allowed
# all three samples also have empty control columns (allowed)
def test_illumina_bam_instrument_notunique():

    a = gpas_uploader.UploadBatch('tests/files/illumina-bam-upload-csv-fail-1.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is not valid
    assert not a.valid

    # there is only a single error
    assert len(a.validation_errors) == 1

    assert list(a.validation_errors.error_message) == ['instrument_platform must be unique']

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "instrument_platform must be unique"
          }
        ]
      }
    }

def test_illumina_bam_files_donotexist():

    a = gpas_uploader.UploadBatch('tests/files/illumina-bam-upload-csv-fail-2.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is not valid
    assert not a.valid

    # there is only a single error
    assert len(a.validation_errors) == 1

    assert list(a.validation_errors.error_message) == ['paired5.bam does not exist']

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample5",
            "error": "paired5.bam does not exist"
          }
        ]
      }
    }

def test_illumina_bam_files_zero_bytes():

    a = gpas_uploader.UploadBatch('tests/files/illumina-bam-upload-csv-fail-3.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is not valid
    assert not a.valid

    # there is only a single error
    assert len(a.validation_errors) == 1

    # which is that one of the bam files does not exist
    assert list(a.validation_errors.error_message) == ['paired4.bam is too small (< 100 bytes)']

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample3",
            "error": "paired4.bam is too small (< 100 bytes)"
          }
        ]
      }
    }


def test_nanopore_bam_check_fails_1():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-bam-upload-csv-fail-1.csv', tags_file='tests/files/tags.txt', run_parallel=False)

    assert a.instantiated

    a.validate()

    # this spreadsheet is not valid!
    assert not a.valid

    # there should be 7 different errors
    # assert len(a.validation_errors) == 8

    assert a.validation_json == {
          "validation": {
            "status": "failure",
            "samples": [
              {
                "sample": "sample1",
                "error": "batch can only contain characters (A-Za-z0-9._-)"
              },
              {
                "sample": "sample1",
                "error": "neg in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative"
              },
              {
                "sample": "sample1",
                "error": "collection_date cannot be in the future"
              },
              {
                "sample": "sample1",
                "error": "Finistere is not a valid ISO-3166-2 region"
              },
              {
                "sample": "sample1",
                "error": "host can only contain the keyword human"
              },
              {
                "sample": "sample1",
                "error": "specimen_organism can only contain the keyword SARS-CoV-2"
              },
              {
                "sample": "sample1",
                "error": "primer_scheme can only contain the keyword auto"
              },
              {
                "sample": "sample1",
                "error": "tags do not validate"
              }
            ]
          }
        }

def test_nanopore_bam_check_fails_2():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-bam-upload-csv-fail-2.csv', run_parallel=False)

    assert a.instantiated

    a.validate()

    # this spreadsheet is not valid!
    assert not a.valid

    # there should be 7 different errors
    # assert len(a.validation_errors) == 8

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample2",
            "error": "unpaired2.bam is too small (< 100 bytes)"
          }
        ]
      }
    }


def test_nanopore_bam_check_fails_3():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-bam-upload-csv-fail-3.csv', run_parallel=False)

    assert a.instantiated

    a.validate()

    # this spreadsheet is not valid!
    assert not a.valid

    # there should be 7 different errors
    # assert len(a.validation_errors) == 8

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": 0,
            "error": "sample_name can only contain characters (A-Za-z0-9._-)"
          },
          {
            "sample": "sam(ple1",
            "error": "batch can only contain characters (A-Za-z0-9._-)"
          },
          {
            "sample": "sam(ple1",
            "error": "run_number can only contain characters (A-Za-z0-9._-)"
          },
          {
            "sample": "sam(ple1",
            "error": "neg in the control field is not valid: field must be either empty or contain the one of the keywords positive or negative"
          },
          {
            "sample": "sam(ple1",
            "error": "collection_date cannot be in the future"
          },
          {
            "sample": "sam(ple1",
            "error": "Finistere is not a valid ISO-3166-2 region"
          },
          {
            "sample": "sam(ple1",
            "error": "host can only contain the keyword human"
          },
          {
            "sample": "sam(ple1",
            "error": "specimen_organism can only contain the keyword SARS-CoV-2"
          },
          {
            "sample": "sam(ple1",
            "error": "primer_scheme can only contain the keyword auto"
          }
        ]
      }
    }


# check an upload CSV where batch is incorrectly called batch_name
def test_illumina_fastq_fail_1():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-1.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert not a.valid

    # there is only a single error
    assert len(a.validation_errors) == 1

    assert any(a.validation_errors['error_message'].str.contains('column batch missing from upload CSV'))

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "column batch missing from upload CSV"
          }
        ]
      }
    }

# check an upload CSV where one of the pair of input FASTQ files is zero-byted
def test_illumina_fastq_fail_2():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-2.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert not a.valid

    assert len(a.validation_errors) == 2

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample4",
            "error": "sample4_1.fastq.gz does not exist"
          },
          {
            "sample": "sample4",
            "error": "sample4_2.fastq.gz does not exist"
          }
        ]
      }
    }


# check an upload CSV where it has no header
def test_illumina_fastq_fail_3():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-3.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert not a.valid

    # because so much depends on sample_name it will only report this error
    assert len(a.validation_errors) == 1

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "no sample_name column in upload CSV"
          }
        ]
      }
    }

# check an upload CSV where there is a header but no samples
def test_illumina_fastq_fail_4():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-4.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert not a.valid

    # the code is smart enough to recognise this and report the specific case
    assert len(a.validation_errors) == 1

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "no samples in upload CSV"
          }
        ]
      }
    }

# check an upload CSV which is not UTF-8 (this is UTF-16)
def test_illumina_fastq_fail_5():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-5.csv', output_json=True)

    # this spreadsheet is valid
    assert not a.instantiated

    assert a.instantiation_json == {
      "instantiation": {
        "status": "failure",
        "message": "upload CSV is not UTF-8"
      }
    }

# check an upload CSV which does not exist!
def test_illumina_fastq_fail_5():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-5000.csv', output_json=True)

    # this spreadsheet is valid
    assert not a.instantiated

    assert a.instantiation_json == {
      "instantiation": {
        "status": "failure",
        "message": "upload CSV does not exist"
      }
    }

# check an upload CSV which has the countries spelt out long form
def test_illumina_fastq_fail_6():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-6.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert len(a.validation_errors) == 3

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample1",
            "error": "United States of America is not a valid ISO-3166-1 country"
          },
          {
            "sample": "sample2",
            "error": "France is not a valid ISO-3166-1 country"
          },
          {
            "sample": "sample3",
            "error": "United Kingdom is not a valid ISO-3166-1 country"
          }
        ]
      }
    }

# check an upload CSV which has the host header deleted (but the sample rows correct)
def test_illumina_fastq_fail_7():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-7.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    # there will be a cascade of errors so build the full list
    errors = [i['error'] for i in a.validation_json['validation']['samples']]

    # and check that the host missing error is included
    assert 'column host missing from upload CSV' in errors

# check an upload CSV where two of the three rows do not have any tags
def test_illumina_fastq_fail_8():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-8.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert len(a.validation_errors) == 2

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample1",
            "error": "tags cannot be empty"
          },
          {
            "sample": "sample2",
            "error": "tags cannot be empty"
          }
        ]
      }
    }

# check an upload CSV where lots of the fields are empty (but should not be)
def test_illumina_fastq_fail_9():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-9.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample2",
            "error": "batch cannot be empty"
          },
          {
            "sample": "sample1",
            "error": "collection_date cannot be empty"
          },
          {
            "sample": None,
            "error": "collection_date must be in form YYYY-MM-DD and cannot include the time"
          },
          {
            "sample": "sample3",
            "error": "country cannot be empty"
          },
          {
            "sample": "sample1",
            "error": "tags cannot be empty"
          },
          {
            "sample": "sample2",
            "error": "tags cannot be empty"
          },
          {
            "sample": "sample1",
            "error": "host cannot be empty"
          },
          {
            "sample": "sample2",
            "error": "specimen_organism cannot be empty"
          },
          {
            "sample": "sample3",
            "error": "primer_scheme cannot be empty"
          },
          {
            "sample": "sample2",
            "error": "instrument_platform cannot be empty"
          },
          {
            "sample": None,
            "error": "instrument_platform must be unique"
          }
        ]
      }
    }

# check an upload CSV where one of the sample_names is empty
def test_illumina_fastq_fail_10():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-10.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "sample_name cannot be empty"
          }
        ]
      }
    }

# check an upload CSV where sample_names is not unique
def test_illumina_fastq_fail_11():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-11.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "sample_name must be unique"
          }
        ]
      }
    }

# check an upload CSV where one pair of fastq files is not given and the other don't exist
def test_illumina_fastq_fail_12():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-12.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample2",
            "error": "fastq1 not specified"
          },
          {
            "sample": "sample3",
            "error": "paired30_1.fastq.gz does not exist"
          },
          {
            "sample": "sample2",
            "error": "fastq2 not specified"
          },
          {
            "sample": "sample3",
            "error": "paired30_2.fastq.gz does not exist"
          },
          {
            "sample": "sample2",
            "error": "fastq1 cannot be empty"
          },
          {
            "sample": "sample2",
            "error": "fastq2 cannot be empty"
          }
        ]
      }
    }

# check an upload CSV where two fastq files exist but use disallowed characters
def test_illumina_fastq_fail_13():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-13.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample2",
            "error": "fastq1 can only contain characters (A-Za-z0-9/._-)"
          },
          {
            "sample": "sample3",
            "error": "fastq1 can only contain characters (A-Za-z0-9/._-)"
          }
        ]
      }
    }


# check an upload CSV where two fastq files exist but use disallowed characters
def test_illumina_fastq_fail_14():

    a = gpas_uploader.UploadBatch('tests/files/illumina-fastq-upload-csv-fail-14.csv', output_json=True)

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample1",
            "error": "tags are duplicated"
          },
          {
            "sample": "sample3",
            "error": "tags are duplicated"
          }
        ]
      }
    }



def test_nanopore_bam_decontaminate_pass_1():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-bam-upload-csv-pass-1.csv', run_parallel=False)

    assert a.instantiated

    a.validate()

    a.decontaminate(run_parallel=False)

    assert a.decontamination_successful


def test_illumina_bam_decontaminate_pass_1():

    a = gpas_uploader.UploadBatch('tests/files/illumina-bam-upload-csv-pass-1.csv', run_parallel=False)

    assert a.instantiated

    a.validate()

    a.decontaminate(run_parallel=False)

    assert a.decontamination_successful



# check an upload CSV where one of the pair of input FASTQ files is zero-byted
def test_nanopore_fastq_fail_1():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-fail-1.csv')

    assert a.instantiated

    a.validate()

    # this spreadsheet is valid
    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample4",
            "error": "unpaired2.fastq.gz is too small (< 100 bytes)"
          }
        ]
      }
    }

# check an upload CSV where one of the unpaired FASTQ files contains no SARS-CoV-2 reads so the resulting FASTQ file is empty
def test_nanopore_fastq_fail_2():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-fail-2.csv')

    assert a.instantiated

    a.validate()

    assert a.valid

    assert a.validation_json == {
      "validation": {
        "status": "completed",
        "samples": [
          {
            "sample": "sample1",
            "files": [
              "unpaired1.fastq.gz"
            ]
          },
          {
            "sample": "sample5",
            "files": [
              "unpaired3.fastq.gz"
            ]
          }
        ]
      }
    }

    a.decontaminate(run_parallel=False)

    # this spreadsheet is valid
    assert not a.decontamination_successful

    assert a.decontamination_json == {
      "submission": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample5",
            "error": "/tmp/sample5.reads.fastq.gz is too small (< 100 bytes)"
          }
        ]
      }
    }

# check an upload CSV which has inconsistent fastq and instruments, here fastq and Illumina
def test_nanopore_fastq_fail_3():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-fail-3.csv')

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "FASTQ file columns and instrument_platform are inconsistent"
          }
        ]
      }
    }


# check an upload CSV which has inconsistent fastq and instruments e.g. fastq1,fastq2 and Nanopore
def test_nanopore_fastq_fail_4():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-fail-4.csv')

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "FASTQ file columns and instrument_platform are inconsistent"
          }
        ]
      }
    }


# check an upload CSV which is in the old format!
def test_nanopore_fastq_fail_5():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-fail-5.csv')

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": None,
            "error": "upload CSV in old format; please provide in new format"
          }
        ]
      }
    }

def test_illumina_fastq_fail_2():

    a = gpas_uploader.UploadBatch('tests/files/nanopore-fastq-upload-csv-pass-1.csv', tags_file='tests/files/badtags.txt')

    assert a.instantiated

    a.validate()

    assert not a.valid

    assert a.validation_json == {
      "validation": {
        "status": "failure",
        "samples": [
          {
            "sample": "sample1",
            "error": "tags do not validate"
          }
        ]
      }
    }
