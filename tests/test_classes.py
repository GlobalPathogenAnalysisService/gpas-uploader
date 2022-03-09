import pytest, pathlib

import gpas_uploader

def test_illumina_bam_pass_1():

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-pass-1.csv')

    # this spreadsheet is valid
    assert a.valid()


def test_nanopore_bam_pass_1():

    a = gpas_uploader.Batch('tests/files/nanopore-bam-upload-csv-pass-1.csv')

    # this spreadsheet is valid
    assert a.valid()

def test_illumina_bam_instrument_notunique():

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-fail-1.csv')

    # this spreadsheet is not valid
    assert not a.valid()

    # there is only a single error
    assert len(a.errors) == 1



def test_illumina_bam_files_donotexist():

    a = gpas_uploader.Batch('tests/files/illumina-bam-upload-csv-fail-2.csv')

    # this spreadsheet is not valid
    assert not a.valid()

    # there is only a single error
    assert len(a.errors) == 1

    # which is that one of the bam files does not exist
    assert list(a.errors.error_message) == ['bam does not exist']


def test_nanopore_bam_check_fails_1():

    a = gpas_uploader.Batch('tests/files/nanopore-bam-upload-csv-fail-1.csv')

    # this spreadsheet is not valid!
    assert not a.valid()

    # there should be 11 different errors
    assert len(a.errors) == 13

    # two of the errors apply to the whole sheet
    assert len(a.errors[a.errors.index.isna()]) == 2


def test_illumina_fastq_pass_1():

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-pass-1.csv')

    # this spreadsheet is valid
    assert a.valid()


# check an upload CSV where the run_number is null
def test_illumina_fastq_pass_2():

    a = gpas_uploader.Batch('tests/files/illumina-fastq-upload-csv-pass-2.csv')

    # this spreadsheet is valid
    assert a.valid()

# def test_decontaminate_illumina_sample_correctly_fails():
#
#     with pytest.raises(Exception) as e_info:
#         decontamination_instance=Decontamination('examples/none_existent_file_1.fastq.gz',\
#                                                  'examples/none_existent_file_2.fastq.gz',\
#                                                  sample='MN908947')
#
#     with pytest.raises(Exception) as e_info:
#         decontamination_instance=Decontamination('examples/MN908947_1.fastq.gz',\
#                                                  'examples/none_existent_file_2.fastq.gz',\
#                                                  sample='MN908947')
#
#
# def test_decontaminate_illumina_sample():
#
#     decontamination_instance=Decontamination('examples/MN908947_1.fastq.gz',\
#                                              'examples/MN908947_2.fastq.gz',\
#                                              sample='MN908947')
#
#     # check that the instance of the class has two variables, both of which are Path objects describing
#     # where to find the paired Illumina FASTQ files
#     assert hasattr(decontamination_instance,'fq1')
#     assert hasattr(decontamination_instance,'fq2')
#     assert isinstance(decontamination_instance.fq1, pathlib.Path)
#     assert isinstance(decontamination_instance.fq2, pathlib.Path)
#
#     # if successful, the result() method should return a tuple of the FASTQ files
#     assert(decontamination_instance.result() == (decontamination_instance.fq1, decontamination_instance.fq2))
#
#
# def test_decontaminate_nanopore_sample_correctly_fails():
#
#     with pytest.raises(Exception) as e_info:
#         decontamination_instance=Decontamination('examples/none_existent_file.fastq.gz',\
#                                                  sample='MN908947')
#
# def test_decontaminate_nanopore_sample():
#
#     decontamination_instance=Decontamination('examples/MN908947.fastq.gz',\
#                                              sample='MN908947')
#
#     # check that the instance of the class has a single Path variable describing
#     # where to find the unpaired Nanopore FASTQ file
#     assert hasattr(decontamination_instance,'fq1')
#     # be a touch paranoid and insist that it does NOT have a second FASTQ file
#     assert ~hasattr(decontamination_instance,'fq2')
#     assert isinstance(decontamination_instance.fq1, pathlib.Path)
#
#     # if successful, the result() method should return the Path object describing where to find the FASTQ
#     assert(decontamination_instance.result() == decontamination_instance.fq1)
