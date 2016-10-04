#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters. Usage: ./genHeader.sh <storm-core.jar filepath>"
    exit 1
fi

if [ ! -f "$1" ]
then
    echo "$1 not found."
    exit 2
fi

javah -d impl/ -classpath $1 org.apache.storm.container.cgroup.monitor.CgroupOOMMonitor
cat ../../java_license_header.txt impl/org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor.h > impl/org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor.h.tmp
mv -f impl/org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor.h.tmp impl/org_apache_storm_container_cgroup_monitor_CgroupOOMMonitor.h
