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
package org.apache.storm.daemon.supervisor.timer;

import org.apache.commons.io.FileUtils;
import org.apache.storm.generated.LocalStateData;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.generated.SupervisorWorkerStats;
import org.apache.storm.generated.LSWorkerStats;
import org.apache.storm.generated.LocalAssignment;
import org.apache.storm.generated.WorkerStats;
import org.apache.storm.utils.TimeseriesStore;
import org.apache.storm.utils.LocalState;
import org.apache.storm.generated.ThriftSerializedObject;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.storm.metric.StatsPusher;

public class WorkerStatsTimer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerStatsTimer.class);

    private final IStormClusterState stormClusterState;
    private final String supervisorId;
    private final Map<String, Object> conf;
    private final Supervisor supervisor;
    private final StatsPusher statsPusher;

    public WorkerStatsTimer(Map<String, Object> conf, Supervisor supervisor) {
        this.stormClusterState = supervisor.getStormClusterState();
        this.supervisorId = supervisor.getId();
        this.supervisor = supervisor;
        this.conf = conf;
        this.statsPusher = new StatsPusher(this.supervisorId);
        statsPusher.prepare(conf);
        // I may need to create a new directory or create multiple instances of TimeseriesStore (one per worker)

    }

    private SupervisorWorkerStats buildWorkerStats(Map<String, Object> conf, Supervisor supervisor) {
        //SupervisorWorkerStats superWorkerStats = new SupervisorWorkerStats(supervisor.getHostName());
        //Localizer localizer = supervisor.getLocalizer();
        //superWorkerStats.set_metrics(localizer.getWorkerStats());
        return null;
    }


    private TBase deserialize(ThriftSerializedObject obj, TDeserializer td) {
        try {
            Class<?> clazz = Class.forName(obj.get_name());
            TBase instance = (TBase) clazz.newInstance();
            td.deserialize(instance, obj.get_bits());
            return instance;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
    public List<LSWorkerStats> getWorkerStats(TimeseriesStore ts) {
        try{
        List<LSWorkerStats> result = new ArrayList<LSWorkerStats>();
        String latestPath = ts.mostRecentVersionPath();
        LOG.info("Getting worker stats from {}", latestPath);

        TDeserializer td = new TDeserializer();
        Map<String, ThriftSerializedObject> point = new HashMap<>();
        String previousPath = null;
        while (latestPath != null) {
            if (previousPath == latestPath) {
                break;
            }
            byte[] serialized = FileUtils.readFileToByteArray(new File(latestPath));
            if (serialized.length == 0) {
                LOG.warn("LocalState file '{}' contained no data, resetting state", latestPath);
            } else {
                LocalStateData data = new LocalStateData();
                td.deserialize(data, serialized);
                point = data.get_serialized_parts();

                if (point.get("worker-stats") != null) {
                    LSWorkerStats stats = (LSWorkerStats)deserialize(point.get("worker-stats"), td);
                    LOG.info("Worker stats are {}", stats);
                    result.add(stats);
                } else {
                    LOG.info("no worker stats..");
                    break;
                }
            }

            previousPath = latestPath;
            // ok better name for this :)
            ts.failVersion(previousPath); // we consumed the version, so remove it
            latestPath = ts.mostRecentVersionPath();
        }
        return result;
        } catch (Exception e) {
            LOG.error("Exception",e);
        }
        return null;
    }

    @Override
    public void run() {
        LocalState localState = supervisor.getLocalState();
        Map<Integer, LocalAssignment> localAssignment = localState.getLocalAssignmentsMap();
        if (localAssignment == null){
            return;
        }

        Map<String, Integer> approvedWorkers = localState.getApprovedWorkers();
        if (approvedWorkers == null) {
            return;
        }

        SupervisorWorkerStats supervisorWorkerStats = new SupervisorWorkerStats();
        supervisorWorkerStats.set_supervisor_id(supervisorId);

        String host = "unknown";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe){
            LOG.error ("Unable to get hostname, metrics will have a bad host name", uhe);
        }

        supervisorWorkerStats.set_supervisor_host(host);
        for (String workerId : approvedWorkers.keySet()) {
            try {
                TimeseriesStore ts = new TimeseriesStore(ConfigUtils.absoluteStormLocalDir(conf) + File.separator + 
                                                         "workers" + File.separator +
                                                         workerId + File.separator + 
                                                         "stats");

                Integer port = approvedWorkers.get(workerId);
                List<LSWorkerStats> stats = this.getWorkerStats(ts);

                LocalAssignment workerAssignment = localAssignment.get(port);
                if (workerAssignment == null) {
                    System.out.println("no worker assignment :( for " + port + ": " + localAssignment);
                    continue;
                }

                WorkerStats workerStats = new WorkerStats();
                workerStats.set_storm_id(workerAssignment.get_topology_id());
                workerStats.set_port(port);
                workerStats.set_executor_infos(workerAssignment.get_executors());
                for (LSWorkerStats stat : stats) {
                    workerStats.put_to_metrics(stat.get_time_stamp(), stat);
                }
                supervisorWorkerStats.put_to_worker_stats(workerId, workerStats);
            }catch (IOException ex) {System.out.println(ex);}
        }
        if (supervisorWorkerStats.get_worker_stats() != null) {
            // no need to send if the stats collection is empty
            statsPusher.sendWorkerStatsToNimbus(supervisorWorkerStats);
        }
    }
}
