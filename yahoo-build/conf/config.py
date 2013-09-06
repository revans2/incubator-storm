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

remappedKeys = {"storm.messaging.netty.buffer.size":"storm.messaging.netty.buffer_size",
                "storm.messaging.netty.max.retries":"storm.messaging.netty.max_retries",
                "storm.messaging.netty.min.wait.ms":"storm.messaging.netty.min_wait_ms",
                "storm.messaging.netty.max.wait.ms":"storm.messaging.netty.max_wait_ms"}

listKeys = set(["storm.auth.simple-white-list.users", "supervisor.slots.ports",
 "storm.zookeeper.servers", "topology.kryo.register", "drpc.servers",
 "worker.childopts", "ui.users", "logs.users", "nimbus.supervisor.users",
 "nimbus.admins", "topology.users"])
mapKeys = set(["isolation.scheduler.machines", "multitenant.scheduler.user.pools", "ui.filter.params"])

allStringKeys = set(["ui.filter.params"])

ignoredKeys = set(["min.user.pid", "storm.zookeeper.auth.payload", "storm.cluster.user", "worker.launcher.group"])

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

def printJavaLibPath(platform):
    if platform.startswith("x86_64"):
        print "java.library.path: \"/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64:\""
    else:
        print "java.library.path: \"/home/y/lib:/usr/local/lib:/usr/lib:/lib:\""

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
    if k in ignoredKeys:
        continue

    if k in remappedKeys:
        k = remappedKeys[k]

    my_normalize = double_quote_if_needed if (k in allStringKeys) else normalize
    if k not in listKeys and k not in mapKeys:
#        print "k not in listkeys"
        print k + ":", my_normalize(v)
    if k in listKeys:
        handleListKey(k,v,my_normalize)
    elif k in mapKeys:
        handleMapKey(k,v, my_normalize)

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
