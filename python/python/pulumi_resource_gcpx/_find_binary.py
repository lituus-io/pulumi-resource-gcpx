# Copyright: Lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

import os
import sysconfig

_BIN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bin")


def _exe_suffix():
    return sysconfig.get_config_var("EXE") or ""


def find_binary():
    path = os.path.join(_BIN_DIR, "pulumi-resource-gcpx" + _exe_suffix())
    if not os.path.isfile(path):
        raise FileNotFoundError(f"binary not found at {path}")
    return path
