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
package backtype.storm.task;

import backtype.storm.generated.NodeInfo;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class WorkerTopologyContext extends GeneralTopologyContext {
    public static final String SHARED_EXECUTOR = "executor";
    
    private Integer _workerPort;
    private List<Integer> _workerTasks;
    private String _codeDir;
    private String _pidDir;
    Map<String, Object> _userResources;
    Map<String, Object> _defaultResources;
    private Map<String, Long> blobToLastKnownVersion;
    //Every value in the map is in the form of "port/node"
    private AtomicReference<Map<Integer, String>> taskToPortNodeStr;
    private String assignmentId;
    
    public WorkerTopologyContext(
            StormTopology topology,
            Map stormConf,
            Map<Integer, String> taskToComponent,
            Map<String, List<Integer>> componentToSortedTasks,
            Map<String, Map<String, Fields>> componentToStreamToFields,
            Map<String, Long> blobToLastKnownVersionShared,
            String stormId,
            String codeDir,
            String pidDir,
            Integer workerPort,
            List<Integer> workerTasks,
            Map<String, Object> defaultResources,
            Map<String, Object> userResources,
            AtomicReference<Map<Integer, String>> taskToPortNodeStr,
            String assignmentId
            ) {
        super(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, stormId);
        _codeDir = codeDir;
        _defaultResources = defaultResources;
        _userResources = userResources;
        blobToLastKnownVersion = blobToLastKnownVersionShared;
        try {
            if(pidDir!=null) {
                _pidDir = new File(pidDir).getCanonicalPath();
            } else {
                _pidDir = null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not get canonical path for " + _pidDir, e);
        }
        _workerPort = workerPort;
        _workerTasks = workerTasks;
        this.taskToPortNodeStr = taskToPortNodeStr;
        this.assignmentId = assignmentId;
    }

    public WorkerTopologyContext(
            StormTopology topology,
            Map stormConf,
            Map<Integer, String> taskToComponent,
            Map<String, List<Integer>> componentToSortedTasks,
            Map<String, Map<String, Fields>> componentToStreamToFields,
            Map<String, Long> blobToLastKnownVersionShared,
            String stormId,
            String codeDir,
            String pidDir,
            Integer workerPort,
            List<Integer> workerTasks,
            Map<String, Object> defaultResources,
            Map<String, Object> userResources
    ) {
        this(topology, stormConf, taskToComponent, componentToSortedTasks, componentToStreamToFields, blobToLastKnownVersionShared,
                stormId, codeDir, pidDir, workerPort, workerTasks, defaultResources, userResources, null, null);
    }

    /**
     * Gets all the task ids that are running in this worker process
     * (including the task for this task).
     */
    public List<Integer> getThisWorkerTasks() {
        return _workerTasks;
    }
    
    public Integer getThisWorkerPort() {
        return _workerPort;
    }

    public String getThisWorkerHost() {
        return assignmentId;
    }

    /**
     * Get a map from task Id to portNode string.
     * Every value in the map is in the form of "port/node". This is for interop with the related code in clojure.
     * @return a map from task To portNode string
     */
    public AtomicReference<Map<Integer, String>> getTaskToPortNodeStr() {
        return taskToPortNodeStr;
    }

    /**
     * Gets the location of the external resources for this worker on the
     * local filesystem. These external resources typically include bolts implemented
     * in other languages, such as Ruby or Python.
     */
    public String getCodeDir() {
        return _codeDir;
    }

    /**
     * If this task spawns any subprocesses, those subprocesses must immediately
     * write their PID to this directory on the local filesystem to ensure that
     * Storm properly destroys that process when the worker is shutdown.
     */
    public String getPIDDir() {
        return _pidDir;
    }
    
    public Object getResource(String name) {
        return _userResources.get(name);
    }
    
    public ExecutorService getSharedExecutor() {
        return (ExecutorService) _defaultResources.get(SHARED_EXECUTOR);
    }

    public Map<String, Long> getBlobToLastKnownVersion() {
        return blobToLastKnownVersion;
    }
}
