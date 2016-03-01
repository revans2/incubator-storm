#!/bin/env python

import os
import re
import json
import types

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
                "storm.messaging.netty.max.wait.ms":"storm.messaging.netty.max_wait_ms",
                "storm.messaging.netty.server.worker.threads":"storm.messaging.netty.server_worker_threads",
                "storm.messaging.netty.client.worker.threads":"storm.messaging.netty.client_worker_threads",
                "topology.http.spout.port.range":"topology.http_spout.port.range",
                "nimbus.thrift.max.buffer.size":"nimbus.thrift.max_buffer_size"}

listKeys = set(["storm.auth.simple-white-list.users", "supervisor.slots.ports",
 "storm.zookeeper.servers", "topology.kryo.register", "drpc.servers",
 "worker.childopts", "ui.users", "ui.groups", "logs.users", "logs.groups", "nimbus.supervisor.users",
 "nimbus.admins", "topology.users", "topology.groups", "nimbus.credential.renewers.classes",
 "topology.auto-credentials", "pacemaker.kerberos.users", "pacemaker.servers"])
mapKeys = set(["isolation.scheduler.machines", "ui.filter.params", "drpc.http.filter.params",
               "supervisor.scheduler.meta", "storm.group.mapping.service.params"])

allStringKeys = set(["ui.filter.params", "drpc.http.filter.params", "storm.group.mapping.service.params"])

ignoredKeys = set(["min.user.pid", "storm.zookeeper.auth.payload", "storm.cluster.user", "worker.launcher.group",
 "multitenant.scheduler.user.pools", "resource.aware.scheduler.user.pools"])

config = dict((k[8:].replace("_", "."), v) for k, v in os.environ.items() \
        if k.startswith("ystorm__") \
        and not k.startswith("ystorm__drpc_auth_acl"))

def toYml(data, indent):
    dt = type(data)
    if dt is types.NoneType:
        ret = "null"
    elif dt is types.BooleanType:
        if data:
            ret = "true"
        else:
            ret = "false"
    elif dt is types.IntType or dt is types.LongType or dt is types.FloatType:
        ret = str(data)
    elif dt is types.StringType or dt is types.UnicodeType:
        ret = "\"" + data.replace("\\","\\\\").replace("\"","\\\"") + "\""
    elif dt is types.TupleType or dt is types.ListType:
        ret = "\n"
        for part in data:
            ret += "    " * indent + "- " + toYml(part, indent+1)+"\n"
    elif dt is types.DictType:
        ret = "\n"
        for k in sorted(data.iterkeys()):
            v = data[k]
            ret += "    " * indent + k + ": "+ toYml(v, indent+1)+"\n"
    else:
        raise "Don't know how to convert %s to YAML type is %s"%(data, dt)
    return ret

def parseValue(value):
    str = value.strip()
    try:
        return json.loads(value)
    except:
        return str

def getJavaLibPath(platform):
    if platform.startswith("x86_64"):
        return "/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64:"
    else:
        return "/home/y/lib:/usr/local/lib:/usr/lib:/lib:"

def splitListValue(v):
    return re.split("[,\s]", v)

result = {}
for k, v in config.items():
    if k in ignoredKeys:
        continue

    if k in remappedKeys:
        k = remappedKeys[k]

    result[k] = parseValue(v)


for listKey in listKeys: 
    if listKey in result:
        val = result[listKey]
        if type(val) in types.StringTypes:
            val = splitListValue(val)
            if not listKey in allStringKeys:
                val = map(parseValue, val)
            result[listKey] = val

for mapKey in mapKeys:
    if mapKey in result:
        val = result[mapKey]
        if type(val) in types.StringTypes:
            items = splitListValue(val)
            val = {}
            for subkey,subval in zip(items[0::2], items[1::2]):
                if mapKey in allStringKeys:
                    val[subkey] = subval
                else:
                    val[subKey] = parseValue(subval)
            result[mapKey] = val

if "supervisor.slots.ports" in result and isinstance(result["supervisor.slots.ports"], int):
    result["supervisor.slots.ports"] = [6700 + x for x in range(0, result["supervisor.slots.ports"])]

if "java.library.path" not in result:
    if "root__platform" in os.environ:
        result["java.library.path"] = getJavaLibPath(os.environ["root__platform"])
    elif "yjava_jdk__platform" in os.environ:
        result["java.library.path"] = getJavaLibPath(os.environ["yjava_jdk__platform"])

print toYml(result, 0)
