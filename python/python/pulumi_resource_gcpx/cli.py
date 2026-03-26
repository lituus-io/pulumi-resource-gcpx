# Copyright: Lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

import os
import sys


def _exec_binary(binary_path):
    if sys.platform == "win32":
        import subprocess
        sys.exit(subprocess.run([binary_path, *sys.argv[1:]]).returncode)
    else:
        os.execvp(binary_path, [binary_path, *sys.argv[1:]])


def main():
    from pulumi_resource_gcpx._find_binary import find_binary
    _exec_binary(find_binary())
