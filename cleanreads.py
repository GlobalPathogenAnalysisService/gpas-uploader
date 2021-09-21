from subprocess import Popen
from error import GpasError
from pathlib import Path

riak = Path("./readItAndKeep").resolve()
ref_genome = "MN908947.fasta"

# test riak installation
with Popen(riak, "--version") as p_test:
    out = p_test.communicate()
    if out.returncode != 0:
        raise GpasError("{'decontamination': 'read removal binary not found'}")


class Decontamination:
    self.process = None

    def __init__(self, fq1, fq2=None, outdir=None):
        if not outdir:
            outdir = Tempfile.dir()
        # test output dir

        if not fq1.exists():
            raise GpasError()

        if fq2:
            if not fq2.exists():
                raise GpasError()
            # paired run

            self.process = Popen(
                riak,
                "--ref_fasta",
                ref_genome,
                "--reads1",
                fq1,
                "--reads2",
                fq2,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        else:
            # single end
            self.process = Popen(
                riak,
                "--ref_fasta",
                ref_genome,
                "--reads1",
                fq1,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

    # returns process id if process properly starts

    def result(self):
        # wait for decontam process to finish
        out, err = self.process.communicate()
        if self.process.returncode != 0:
            raise GpasError("{'decontamination': return_code}")
        return out, err
