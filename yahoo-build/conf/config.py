#!/bin/env python

import os
import re

print """
# This configuration file is controlled by yinst set variables.
# Example:
# $ yinst set ystorm.storm_zookeeper_servers=server1,server2
#
########### These MUST be filled in for a storm configuration
# storm.zookeeper.servers:
#     - "server1"
#     - "server2"
#
# nimbus.host: "server3"
#
#
# ##### These may optionally be filled in:
#
## List of custom serializations
# topology.kryo.register:
#     - org.mycompany.MyType
#     - org.mycompany.MyType2: org.mycompany.MyType2Serializer
#
## Locations of the drpc servers
# drpc.servers:
#     - "server1"
#     - "server2"
#

"""

listKeys = set(["storm.auth.simple-white-list.users", "supervisor.slots.ports", "storm.zookeeper.servers", "topology.kryo.register", "drpc.servers", "worker.childopts"])
mapKeys = set(["isolation.scheduler.machines"])

config = dict((k[8:].replace("_", "."), v) for k, v in os.environ.items() if k.startswith("ystorm__"))

numeric = re.compile("^[0-9\.]+$")

def normalize(value):
    str = value.strip()
    if numeric.search(str):
        return str
    else:
        return "\"" + str + "\""

def printJavaLibPath(platform):
    if platform.startswith("x86_64"):
        print "java.library.path: \"/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64: -cp /home/y/lib/jars/yjava_ysecure.jar:/home/y/lib/jars/yjava_ysecure_native.jar\""
    else:
        print "java.library.path: \"/home/y/lib:/usr/local/lib:/usr/lib:/lib: -cp /home/y/lib/jars/yjava_ysecure.jar:/home/y/lib/jars/yjava_ysecure_native.jar\""

def splitListValue(v):
    return re.split("[,\s]", v)

def handleListKey(k,v):
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
        v_without_quotes = normalize(v)
        print k + ":", "\"", v, "\""

    else:
#        print "in else"
        print k + ":"
        for item in splitListValue(v):
            print "    -", normalize(item)

def handleMapKey(k,v):
    print k + ":"
    items = splitListValue(v)
    for subkey,subval in zip(items[0::2],items[1::2]):
        print "    %s: %s" % (normalize(subkey),normalize(subval))


for k, v in config.items():
#    print "___________Processing: ", k,v
    if k not in listKeys and k not in mapKeys:
#        print "k not in listkeys"
        print k + ":", normalize(v)
    if k in listKeys:
        handleListKey(k,v)
    elif k in mapKeys:
        handleMapKey(k,v)

if "java.library.path" not in config:
    if "root__platform" in os.environ:
        printJavaLibPath(os.environ["root__platform"])
    elif "yjava_jdk__platform" in os.environ:
        printJavaLibPath(os.environ["yjava_jdk__platform"])

if "storm.zookeeper.servers" not in config:
    if "zookeeper_server__quorum" in os.environ:
        print k + ":"
        for item in os.environ["zookeeper_server__quorum"].split(","):
            print "    -", normalize(item)
