import pytest, pathlib

from cleanreads import Decontamination

import gpas_uploader

def test_bam_check_fails():

    a = gpas_uploader.Samplesheet('tests/bam-illumina-upload-csv-fail-0.csv')
    assert len(a.errors) == 2




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
