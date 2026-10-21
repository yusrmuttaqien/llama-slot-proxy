"""Console logger. info() only prints with --verbose; warn()/error() always print."""
import sys
from datetime import datetime


class Logger:
    def __init__(self, verbose=False, out=None, err=None):
        self.verbose = verbose
        self._out = out
        self._err = err

    @staticmethod
    def _stamp():
        return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

    def info(self, msg):
        if self.verbose:
            out = self._out if self._out is not None else sys.stdout
            print(f"{self._stamp()} {msg}", file=out, flush=True)

    def warn(self, msg):
        err = self._err if self._err is not None else sys.stderr
        print(f"{self._stamp()} WARNING: {msg}", file=err, flush=True)

    def error(self, msg):
        err = self._err if self._err is not None else sys.stderr
        print(f"{self._stamp()} ERROR: {msg}", file=err, flush=True)
