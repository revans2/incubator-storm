#!/home/y/bin/python

# We are using /home/y/bin/python as this script needs to use python 2.7.
# When yinst runs, /bin/env python returns /usr/bin/python which
# can be python 2.6 for some deployed versions of ylinux.

import os
import json
import yaml

print """
# This configuration file is controlled by yinst set variables.
# This is for the resource-aware-scheduler

"""

user_resource_pool_key = "resource.aware.scheduler.user.pools"

config = dict((k[8:].replace("_", "."), v) for k, v in os.environ.items() if k.startswith("ystorm__"))

resource_pool_json = config.get(user_resource_pool_key)
if resource_pool_json is not None:
    resource_pool = "";
    try:
        resource_pool = {user_resource_pool_key: json.loads(resource_pool_json)}
    except:
        print "Error occurred in parsing config json!"
        raise

    yml = "";
    try:
        yml = yaml.safe_dump(resource_pool, default_flow_style=False)
    except:
        print "Error occurred in converting to YAML!"
        raise
    print (yml)
