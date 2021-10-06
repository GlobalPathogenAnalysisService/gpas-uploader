from json import dumps
import sys


class GpasError(Exception):
    pass


def emsg(error, json=False, file=sys.stdout):
    if json:
        print(dumps({"error": error}), file=file)
        file.flush()


def smsg(samples, json=False, file=sys.stdout):
    if json:
        print(dumps({"submission": samples}), file=file)
        file.flush()


def dmsg(sample, error, msg=None, json=False, file=sys.stdout):
    if not msg:
        msg = {}
    payload = {"sample": sample.name, "status": error}
    for k in msg:
        payload[k] = msg[k]

    if json:
        print(
            dumps({"decontamination": payload}), file=file,
        )
        file.flush()


def verr(errors, json=False, file=sys.stdout):
    if json:
        print(dumps({"validation": errors}))
        file.flush()


def derr(sample, error, json=False, file=sys.stdout):
    if json:
        print(
            dumps(
                {
                    "decontamination": {
                        "sample": sample,
                        "status": "failure",
                        "error": error,
                    }
                }
            )
        )
        file.flush()
