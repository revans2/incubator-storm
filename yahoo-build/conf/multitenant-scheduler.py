#!/home/y/bin/python

# We are using /home/y/bin/python in case we need to use the
# version of python pulled in by storm's dependencies.
# When yinst runs, /bin/env python returns /usr/bin/python which
# can be a far older version of python.

import os
import re

print """
# This configuration file is controlled by yinst set variables.
# This is for the multitenant-scheduler 

"""
whitelistKeys = set(["multitenant.scheduler.user.pools"])

remappedKeys = {}

listKeys = set([])
mapKeys = set(["multitenant.scheduler.user.pools"])

allStringKeys = set([])

config = dict((k[8:].replace("_", "."), v) for k, v in os.environ.items() if k.startswith("ystorm__"))

qq_string = re.compile("^\".*\"$")
numeric = re.compile("^[0-9\.]+$")
bool_re = re.compile("^(true)|(false)$",re.I)

def double_quote_if_needed(str):
    if qq_string.search(str):
        return str
    return "\"" + str + "\""

def normalize(value):
    str = value.strip()
    if numeric.search(str):
        return str
    elif bool_re.search(str):
        return str
    else:
        return double_quote_if_needed(str)

def splitListValue(v):
    return re.split("[,\s]", v)

def handleListKey(k,v,norm_fn):
    if k == "supervisor.slots.ports":
#        print "in elif 1"
        print k + ":"
        numPorts = os.environ["ystorm__supervisor_slots_ports"]
#        print "***** got numPorts", numPorts
        for num in range(0, int(numPorts)):
            portValue = 6700 + num
            print "    -", portValue

    elif k == "worker.childopts":
#        print "in elif 2"
        v_without_quotes = norm_fn(v)
        print k + ":", "\"", v, "\""

    else:
#        print "in else"
        print k + ":"
        for item in splitListValue(v):
            print "    -", norm_fn(item)

def handleMapKey(k,v,norm_fn):
    print k + ":"
    items = splitListValue(v)
    for subkey,subval in zip(items[0::2],items[1::2]):
        print "    %s: %s" % (norm_fn(subkey),norm_fn(subval))


for k, v in config.items():
#    print "___________Processing: ", k,v
    if not k in whitelistKeys:
        continue

    if k in remappedKeys:
        k = remappedKeys[k]

    my_normalize = normalize
    if k in allStringKeys:
        my_normalize = double_quote_if_needed

    if k not in listKeys and k not in mapKeys:
#        print "k not in listkeys"
        print k + ":", my_normalize(v)
    if k in listKeys:
        handleListKey(k,v,my_normalize)
    elif k in mapKeys:
        handleMapKey(k,v, my_normalize)


