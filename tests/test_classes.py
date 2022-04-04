import pytest, pathlib

import gpas_uploader

def test_nonASCII_upload_csv_fails():

    with pytest.raises(Exception) as e_info:
        validate.Batch("tests/files/bam-nanopore-samplesheet ~ - ! 'template-good.csv")

def test_no_upload_csv_fails():

    with pytest.raises(Exception) as e_info:
        validate.Batch('tests/files/none-existent-file.csv')

def test_illumina_fastq_pass_1():

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-pass-1.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-pass-2.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-pass-1.csv', run_parallel=True)

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

    a = gpas_uploader.Batch('tests/files/nanopore-fastq-upload-csv-pass-1.csv')

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

    a = gpas_uploader.Batch('tests/files/nanopore-bam-upload-csv-pass-1.csv', run_parallel=True)

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

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-fail-1.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-fail-2.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-fail-3.csv')

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

    a = gpas_uploader.Batch('tests/files/nanopore-bam-upload-csv-fail-1.csv', tags_file='tests/files/tags.txt', run_parallel=True)

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

# check an upload CSV where batch is incorrectly called batch_name
def test_illumina_fastq_fail_1():

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-fail-1.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-fail-2.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-fail-3.csv')

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

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-fail-4.csv')

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

def test_nanopore_bam_decontaminate_pass_1():

    a = gpas_uploader.Batch('tests/files/nanopore-bam-upload-csv-pass-1.csv', run_parallel=True)

    a.validate()

    a.decontaminate(run_parallel=True)

    assert a.decontamination_successful


def test_illumina_bam_decontaminate_pass_1():

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-pass-1.csv', run_parallel=True)

    a.validate()

    a.decontaminate(run_parallel=True)

    assert a.decontamination_successful



# check an upload CSV where one of the pair of input FASTQ files is zero-byted
def test_nanopore_fastq_fail_1():

    a = gpas_uploader.Batch('tests/files/nanopore-fastq-upload-csv-fail-1.csv')

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
def test_illumina_fastq_fail_1():

    a = gpas_uploader.Batch('tests/files/nanopore-fastq-upload-csv-fail-2.csv')

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

    a.decontaminate(run_parallel=True)

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

def test_illumina_fastq_fail_2():

    a = gpas_uploader.Batch('tests/files/nanopore-fastq-upload-csv-pass-1.csv', tags_file='tests/files/badtags.txt')

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
