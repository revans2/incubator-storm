/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.container.cgroup.monitor;

import org.apache.storm.container.cgroup.CgroupCommon;
import org.apache.storm.container.cgroup.core.MemoryCore;

public class CgroupMonitoringInfo implements Comparable<CgroupMonitoringInfo> {
    public final String cgroupId;
    public final String path;
    //memory.oom_control file descriptor for this cgroup
    private int ofd;
    //cgroup.event_control file descriptor for this cgroup
    private int efd;

    //the address of the the pointer
    private long eventDataPtr;

    public CgroupMonitoringInfo(String cgroupId, String path) {
        this.cgroupId = cgroupId;
        this.path = path;
    }

    public String getFullPath() {
        return this.path + "/" + this.cgroupId;
    }

    public String getFullPathToOOMControlFile() {
        return this.getFullPath() + MemoryCore.MEMORY_OOM_CONTROL;
    }

    public String getFullPathToCgroupEventControlFile() {
        return this.getFullPath() + CgroupCommon.CGROUP_EVENT_CONTROL;
    }

    public int getOfd() {
        return this.ofd;
    }

    public void setOfd(int ofd) {
        this.ofd = ofd;
    }

    public int getEfd() {
        return this.efd;
    }

    public void setEfd(int efd) {
        this.efd = efd;
    }

    public long getEventDataPtr() {
        return this.eventDataPtr;
    }

    public void setEventDataPtr(long eventDataPtr) {
        this.eventDataPtr = eventDataPtr;
    }

    @Override
    public int hashCode() {
        return this.cgroupId.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CgroupMonitoringInfo) {
            return this.cgroupId.equals(((CgroupMonitoringInfo) obj).cgroupId);
        }
        return false;
    }

    @Override
    public String toString() {
        return this.cgroupId + " efd: " + this.getEfd() + " ofd: " + this.getOfd();
    }

    @Override
    public int compareTo(CgroupMonitoringInfo o) {
        return this.cgroupId.compareTo(o.cgroupId);
    }
}
