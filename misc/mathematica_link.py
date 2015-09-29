#! /usr/bin/env python3

import subprocess
import warnings
import json
import time
import numpy as np
from matplotlib import pyplot as plt

class MathematicaProcess:

    def __init__(self):
        self._process = subprocess.Popen(args = ["math", "-noprompt"], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        # make sure the kernel is started.
        self("Null")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        if self._process is not None:
            warnings.warn("Subprocess was still active on deletion of the MathematicaProcess. Please close it explicitly.")
            self.close()

    def write(self, s):
        assert isinstance(s, str)
        if not s.endswith("\n"):
            s += "\n"
        self._process.stdin.write(s.encode("utf-8"))
        self._process.stdin.flush()

    def readline(self):
        return self._process.stdout.readline().decode("utf-8")

    def close(self):
        assert self._process is not None
        self._process.stdin.close()
        self._process.wait()
        self._process = None

    def __call__(self, expression):

        command = "ToCharacterCode[ExportString[{}, \"JSON\"]]".format(expression)
        self.write(command)

        response = self.readline()

        response = response.rstrip("\r\n")
        assert response.startswith("{")
        assert response.endswith("}")
        response = response[1:-1]

        response = "".join([chr(int(cc)) for cc in response.split(",")])
        response = json.loads(response)

        return response

def main():
    data = []
    with MathematicaProcess() as mathematica:
        while len(data) < 40:
            d = np.random.randint(100, 100000)
            print(len(data), d)
            t1 = time.time()
            expression = "RealDigits[Pi, 10, {}]".format(d)
            result = mathematica(expression)
            t2 = time.time()
            duration = (t2 - t1)
            data.append((d, duration))

    (d, duration) = np.array(data).transpose()

    plt.plot(d, duration, "*")
    plt.show()

if __name__ == "__main__":
    main()

print("done.")
