#!/home/y/bin/python

# We are using /home/y/bin/python in case we need to use the
# version of python pulled in by storm's dependencies.
# When yinst runs, /bin/env python returns /usr/bin/python which
# can be a far older version of python.

import os
import re

print """
# This configuration file is controlled by yinst set variables.
# This is for the DRPCSimpleAclAuthorizer
"""

CLIENT_KEY='client.users'
INV_KEY='invocation.user'

drpc_acl_re = re.compile("^drpc.auth.acl.([^. ]+).")
clients_re = re.compile("^drpc.auth.acl.[^. ]+."+CLIENT_KEY)
invocation_re = re.compile("^drpc.auth.acl.[^. ]+."+INV_KEY)

config = dict((k[8:].replace("_", "."), v) for k, v in os.environ.items() \
        if k.startswith("ystorm__drpc_auth_acl_"))

qq_string = re.compile("^\".*\"$")

def get_function_from_key(k):
    match = drpc_acl_re.match(k)
    return match.group(1)

def double_quote_if_needed(str):
    if qq_string.match(str):
        return str
    return '"' + str + '"'

def splitListValue(v):
    return re.split("[,\s]", v)

def handleClientsKey(v,norm_fn):
    return [norm_fn(item) for item in splitListValue(v)]

def groupSettingsByFunction(config):
    toRet = {}
    for k, v in config.items():
        function = get_function_from_key(k)
        if not function:
            continue

        if function not in toRet.keys():
            toRet[function] = {}
        if clients_re.match(k):
            toRet[function][CLIENT_KEY] = \
                    handleClientsKey(v,double_quote_if_needed)
        elif invocation_re.match(k):
            toRet[function][INV_KEY] = double_quote_if_needed(v)
        else:
            print "# SKIPPED unknown key %s=%s"%(k,v)

    return toRet

def printConfig(map):
    print 'drpc.authorizer.acl:'
    for func in map.keys():
        if CLIENT_KEY in map[func] or INV_KEY in map[func]:
            print '  %s:'%func
            if CLIENT_KEY in map[func]:
                print '    %s:'%CLIENT_KEY
                for user in map[func][CLIENT_KEY]:
                    print '      -', user
            if INV_KEY in map[func]:
                print '    %s:'%INV_KEY, map[func][INV_KEY]

map = groupSettingsByFunction(config)
printConfig(map)
