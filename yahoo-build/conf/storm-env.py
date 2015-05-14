#!/bin/env python

import os
import sys

if "ystorm__JAVA_HOME" in os.environ:
    sys.stdout.write("export JAVA_HOME='")
    sys.stdout.write(os.environ["ystorm__JAVA_HOME"])
    sys.stdout.write("';\n")
