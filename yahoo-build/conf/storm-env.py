#!/home/y/bin/python

# We are using /home/y/bin/python in case we need to use the
# version of python pulled in by storm's dependencies.
# When yinst runs, /bin/env python returns /usr/bin/python which
# can be a far older version of python.

import os
import sys

if "ystorm__JAVA_HOME" in os.environ:
    sys.stdout.write("export JAVA_HOME='")
    sys.stdout.write(os.environ["ystorm__JAVA_HOME"])
    sys.stdout.write("';\n")
