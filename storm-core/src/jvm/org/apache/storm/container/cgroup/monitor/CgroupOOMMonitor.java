/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.container.cgroup.monitor;

import backtype.storm.Config;
import backtype.storm.cluster.ClusterStateContext;
import backtype.storm.cluster.DaemonType;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.LocalAssignment;
import backtype.storm.generated.WorkerResources;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Time;
import backtype.storm.utils.Utils;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.container.cgroup.CgroupCommon;
import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.utils.ConfigUtils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class CgroupOOMMonitor {

    static {
        // make sure this on the java.library.path
        System.loadLibrary("cgroupOOMMonitor"); // Load native library at runtime
    }

    // initialize java api calls from native
    native int initializeJavaApiCallIds();

    // Monitor OOMs
    native int startOOMMonitoringNative(ConcurrentMap<String, CgroupMonitoringInfo> cgroupsBeingMonitored,
                                        ConcurrentLinkedQueue<String> notifications);

    // Register Cgroup for OOM monitoring
    native int registerCgroupNative(ConcurrentLinkedQueue<CgroupMonitoringInfo> cgroupsToAdd,
                                    ConcurrentLinkedQueue<CgroupMonitoringInfo> cgroupsToRemove,
                                    ConcurrentMap<String, CgroupMonitoringInfo> cgroupsBeingMonitored);

    /* member variables */
    public ConcurrentLinkedQueue<CgroupMonitoringInfo> cgroupMonitorAddQueue = new ConcurrentLinkedQueue<CgroupMonitoringInfo>();
    public ConcurrentLinkedQueue<CgroupMonitoringInfo> cgroupMonitorRemoveQueue = new ConcurrentLinkedQueue<CgroupMonitoringInfo>();
    public ConcurrentLinkedQueue<String> notifications = new ConcurrentLinkedQueue<String>();
    public ConcurrentMap<String, CgroupMonitoringInfo> cgroupsBeingMonitored = new ConcurrentHashMap<String, CgroupMonitoringInfo>();
    Map<String, Object> conf = new HashMap<String, Object>();

    IStormClusterState stormClusterState;

    private static final Logger LOG = LoggerFactory.getLogger(CgroupOOMMonitor.class);

    public CgroupOOMMonitor(Map conf) {
       this(conf, null);
    }

    public CgroupOOMMonitor(Map conf, IStormClusterState stormClusterState) {

        this.conf.putAll(conf);
        if (initializeJavaApiCallIds() != 0) {
            throw new RuntimeException("Failed to initialize java api call ids for native!");
        }

        if (stormClusterState == null) {
            this.stormClusterState = getStormClusterState(this.conf);
        } else {
            this.stormClusterState = stormClusterState;
        }
        // Start thread
        this.startMonitoringThread();
    }

    static IStormClusterState getStormClusterState(Map conf) {
        List<ACL> acls = null;
        if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
            acls = SupervisorUtils.supervisorZkAcls();
        }
        try {
            return ClusterUtils.mkStormClusterState(conf, acls, new ClusterStateContext(DaemonType.SUPERVISOR));
        } catch (Exception e) {
            LOG.error("supervisor can't create stormClusterState");
            throw Utils.wrapInRuntime(e);
        }
    }

    public void startMonitoringThread() {
        Thread monitoringThread = new Thread() {
            @Override
            public void run() {
                LOG.info("Starting cgroup OOM monitoring...");
                while (true) {
                    try {
                        if (startOOMMonitoringNative(cgroupsBeingMonitored, notifications) != 0) {
                            throw new RuntimeException("cgroup oom monitor registering thread failed!");
                        }
                    } catch (Exception e) {
                        LOG.error("Exception thrown in OOM monitoring thread! Will retry in 1 seconds", e);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.error("Exception thrown while trying to sleep", e);
                    }
                }
            }
        };
        monitoringThread.start();
    }

    public void startNotificationThread() {
        final Thread notificationThread = new Thread() {
            @Override
            public void run() {
                int maxReports = Utils.getInt(conf.get(Config.TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL));
                int interval = Utils.getInt(conf.get(Config.TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS));
                int numReports = maxReports;
                long startTime = Time.currentTimeSecs();
                while (true) {
                    if ((Time.currentTimeSecs() - startTime) > interval) {
                        startTime = Time.currentTimeSecs();
                        numReports = maxReports;
                    }
                    while (!notifications.isEmpty() && numReports > 0) {

                        LOG.debug("# of pending notifications: {}", notifications.size());
                        String workerId = notifications.poll();
                        LocalState localState = null;
                        try {
                            localState = ConfigUtils.supervisorState(conf);
                        } catch (IOException e) {
                            LOG.warn("Failed to read local state for worker: {}", workerId, e);
                        }

                        String hostname = null;
                        try {
                            hostname = Utils.hostname(conf);
                        } catch (UnknownHostException e) {
                            LOG.warn("Could not get hostname where worker {} resides", workerId);
                        }
                        Integer port = null;
                        LocalAssignment localAssignment = null;
                        Map<String, Integer> approvedWorkers = localState.getApprovedWorkers();
                        if (approvedWorkers.containsKey(workerId)) {
                            port = localState.getApprovedWorkers().get(workerId);
                            Map<Integer, LocalAssignment> localAssignmentMap = localState.getLocalAssignmentsMap();
                            if (localAssignmentMap.containsKey(port)) {
                                localAssignment = localAssignmentMap.get(port);
                            }
                        }

                        String location = port == null ? hostname : hostname + ":" + port;

                        String assignments = "Unknown";
                        String resourceAllocation = "Unknown";
                        if (localAssignment != null) {
                            StringBuilder assignmentsStringBuilder = new StringBuilder();
                            for (ExecutorInfo executorInfo : localAssignment.get_executors()) {
                                assignmentsStringBuilder.append("[" + executorInfo.get_task_start() + " " + executorInfo.get_task_end() + "] ");

                            }
                            assignments = assignmentsStringBuilder.toString();
                            WorkerResources workerResources = localAssignment.get_resources();
                            resourceAllocation = "[Memory On-heap: " + workerResources.get_mem_on_heap() + "(MB)"
                                    + " Off-heap: " + workerResources.get_mem_off_heap() + "(MB)] " + "[CPU: " + workerResources.get_cpu() + "(%)]";
                        }

                        String msg = "Cgroup OOM detected for worker " + workerId + " on " + location
                                + " with assignments " + assignments + " and resource allocation " + resourceAllocation
                                + ". Please consider increasing resources allocated to executors running in the worker!\n";
                        //report error to zookeeper
                        if (localAssignment != null && port != null && hostname!=null) {
                            String topoId = localAssignment.get_topology_id();
                            stormClusterState.reportTopologyError(topoId, hostname, port, msg);
                            numReports --;
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.error("Exception thrown while trying to sleep", e);
                    }
                }
            }
        };
        notificationThread.start();
    }

    /**
     * Sets the cgroups to monitor for ooms
     * @param cgroupsToMonitor a set of cgroups to monitor for ooms
     */
    public void setCgroupsToMonitor(Set<CgroupCommon> cgroupsToMonitor) {

        Map<String, CgroupCommon> toMonitorCgroupIds = new HashMap<String, CgroupCommon>();
        for (CgroupCommon cgroupCommon : cgroupsToMonitor) {
            toMonitorCgroupIds.put(cgroupCommon.getName(), cgroupCommon);
        }
        Set<String> currentMonitorCgroupIds = new HashSet<String>(this.cgroupsBeingMonitored.keySet());

        Set<String> toAddToMonitor = new HashSet<String>(toMonitorCgroupIds.keySet());
        toAddToMonitor.removeAll(currentMonitorCgroupIds);

        Set<String> toRemoveFromMonitor = new HashSet<String>(currentMonitorCgroupIds);
        toRemoveFromMonitor.removeAll(toMonitorCgroupIds.keySet());

        // add new cgroups to be monitored
        for (String workerId : toAddToMonitor) {
            CgroupCommon cgroupCommon = toMonitorCgroupIds.get(workerId);
            String path = cgroupCommon.getParent().getDir();
            CgroupMonitoringInfo newCgroupMonitoringInfo = new CgroupMonitoringInfo(workerId, path);
            this.cgroupMonitorAddQueue.add(newCgroupMonitoringInfo);
        }

        // add cgroups to be removed from monitoring
        for (String workerId : toRemoveFromMonitor) {
            CgroupMonitoringInfo info = this.cgroupsBeingMonitored.get(workerId);
            this.cgroupMonitorRemoveQueue.add(info);
        }
        if (registerCgroupNative(this.cgroupMonitorAddQueue, this.cgroupMonitorRemoveQueue, this.cgroupsBeingMonitored) != 0) {
            throw new RuntimeException("Not successful in setting cgroup to OOM monitoring");
        }
    }

    public void addCgroupToMonitor(CgroupCommon cgroup) {
        String name = cgroup.getName();
        String path = cgroup.getParent().getDir();
        CgroupMonitoringInfo cgroupToMonitor = new CgroupMonitoringInfo(name, path);
        if (!this.cgroupsBeingMonitored.containsKey(cgroupToMonitor.cgroupId)) {
            this.cgroupMonitorAddQueue.add(cgroupToMonitor);
            if (registerCgroupNative(this.cgroupMonitorAddQueue, this.cgroupMonitorRemoveQueue, this.cgroupsBeingMonitored) != 0) {
                throw new RuntimeException("Not successful in adding cgroup " + cgroup.getName() + " to OOM monitoring");
            }
        } else {
            LOG.warn("Cannot add cgroup {}!. It is already being monitored", cgroup.getName());
        }
    }

    public void removeCgroupToMonitor(CgroupCommon cgroup) {
        if (this.cgroupsBeingMonitored.containsKey(cgroup.getName())) {
            CgroupMonitoringInfo target = this.cgroupsBeingMonitored.get(cgroup.getName());
            this.cgroupMonitorRemoveQueue.add(target);
            if (registerCgroupNative(this.cgroupMonitorAddQueue, this.cgroupMonitorRemoveQueue, this.cgroupsBeingMonitored) != 0) {
                throw new RuntimeException("Not successful in remove cgroup " + cgroup.getName() + " from OOM monitoring");
            }
        } else {
            LOG.warn("Cannot remove cgroup {}! It is not currently being monitored", cgroup.getName());
        }
    }

    public Set<String> getCgroupsBeingMonitored() {
        return this.cgroupsBeingMonitored.keySet();
    }

    public String getNextNotification() {
        return this.notifications.poll();
    }

    public boolean hasOOMNotification() {
        return !this.notifications.isEmpty();
    }
}
