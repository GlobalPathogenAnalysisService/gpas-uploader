from json import dumps
import sys


class GpasError(Exception):
    pass


def dmsg(sample_name, error, msg=None, json=False, file=sys.stdout):
    if not msg:
        msg = {}
    payload = {"sample": sample_name, "status": error}
    for k in msg:
        payload[k] = msg[k]

    if json:
        print(
            dumps({"decontamination": payload}), file=file,
        )
        file.flush()
