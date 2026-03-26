# Copyright: Lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>

import os
import stat


def test_binary_exists():
    from pulumi_resource_gcpx import find_binary
    path = find_binary()
    assert os.path.isfile(path), f"binary not found at {path}"


def test_binary_executable():
    from pulumi_resource_gcpx import find_binary
    path = find_binary()
    st = os.stat(path)
    assert st.st_mode & stat.S_IXUSR, f"binary is not executable: {path}"
