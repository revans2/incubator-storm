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

package org.apache.storm.container.cgroup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.container.ResourceIsolationInterface;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.CpusetCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.container.cgroup.monitor.CgroupOOMMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

/**
 * Class that implements ResourceIsolationInterface that manages cgroups
 */
public class CgroupManager implements ResourceIsolationInterface {

    private static final Logger LOG = LoggerFactory.getLogger(CgroupManager.class);

    private CgroupCenter center;

    private Hierarchy hierarchy;

    private CgroupCommon rootCgroup;

    private String rootDir;

    private Map<String, Object> conf;

    private CgroupOOMMonitor oomMonitor;

    /**
     * initialize data structures
     * @param conf storm confs
     */
    @Override
    public void prepare(Map<String, Object> conf) throws IOException {
        this.conf = conf;
        this.rootDir = Config.getCgroupRootDir(this.conf);
        if (this.rootDir == null) {
            throw new RuntimeException("Check configuration file. The storm.supervisor.cgroup.rootdir is missing.");
        }

        File file = new File(Config.getCgroupStormHierarchyDir(conf) + "/" + this.rootDir);
        if (!file.exists()) {
            LOG.error("{} does not exist", file.getPath());
            throw new RuntimeException("Check if cgconfig service starts or /etc/cgconfig.conf is consistent with configuration file.");
        }
        this.center = CgroupCenter.getInstance();
        if (this.center == null) {
            throw new RuntimeException("Cgroup error, please check /proc/cgroups");
        }
        this.prepareSubSystem(this.conf);

        //initalize oom monitor
        if (Utils.getBoolean(this.conf.get(Config.STORM_CGROUP_OOM_MONITORING_ENABLE), false)) {
            String libPathProperty = System.getProperty("java.library.path");
            LOG.debug("java.library.path: {}", libPathProperty);
            this.oomMonitor = new CgroupOOMMonitor(conf);
            this.oomMonitor.startNotificationThread();
            // enabling monitoring on exisiting cgroups
            LOG.info("Cgroups to Monitor for OOM on startup: {} ", this.rootCgroup.getChildren());
            this.oomMonitor.setCgroupsToMonitor(this.rootCgroup.getChildren());
        }
    }

    /**
     * Initialize subsystems
     */
    private void prepareSubSystem(Map<String, Object> conf) throws IOException {
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : Config.getCgroupStormResources(conf)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }

        this.hierarchy = center.getHierarchyWithSubSystems(subSystemTypes);

        if (this.hierarchy == null) {
            Set<SubSystemType> types = new HashSet<SubSystemType>();
            types.add(SubSystemType.cpu);
            this.hierarchy = new Hierarchy(Config.getCgroupStormHierarchyName(conf), types, Config.getCgroupStormHierarchyDir(conf));
        }
        this.rootCgroup = new CgroupCommon(this.rootDir, this.hierarchy, this.hierarchy.getRootCgroups());

        // set upper limit to how much cpu can be used by all workers running on supervisor node.
        // This is done so that some cpu cycles will remain free to run the daemons and other miscellaneous OS operations.
        CpuCore supervisorRootCPU = (CpuCore) this.rootCgroup.getCores().get(SubSystemType.cpu);
        setCpuUsageUpperLimit(supervisorRootCPU, ((Number) this.conf.get(Config.SUPERVISOR_CPU_CAPACITY)).intValue());
    }

    /**
     * Use cfs_period & cfs_quota to control the upper limit use of cpu core e.g.
     * If making a process to fully use two cpu cores, set cfs_period_us to
     * 100000 and set cfs_quota_us to 200000
     */
    private void setCpuUsageUpperLimit(CpuCore cpuCore, int cpuCoreUpperLimit) throws IOException {

        if (cpuCoreUpperLimit == -1) {
            // No control of cpu usage
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit);
        } else {
            cpuCore.setCpuCfsPeriodUs(100000);
            cpuCore.setCpuCfsQuotaUs(cpuCoreUpperLimit * 1000);
        }
    }

    @Override
    public void reserveResourcesForWorker(String workerId, Map resourcesMap) throws SecurityException {
        LOG.info("Creating cgroup for worker {} with resources {}", workerId, resourcesMap);
        Number cpuNum = null;
        // The manually set STORM_WORKER_CGROUP_CPU_LIMIT config on supervisor will overwrite resources assigned by RAS (Resource Aware Scheduler)
        if (this.conf.get(Config.STORM_WORKER_CGROUP_CPU_LIMIT) != null) {
            cpuNum = (Number) this.conf.get(Config.STORM_WORKER_CGROUP_CPU_LIMIT);
        } else if(resourcesMap.get("cpu") != null) {
            cpuNum = (Number) resourcesMap.get("cpu");
        }

        Number totalMem = null;
        // The manually set STORM_WORKER_CGROUP_MEMORY_MB_LIMIT config on supervisor will overwrite resources assigned by RAS (Resource Aware Scheduler)
        if (this.conf.get(Config.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT) != null) {
            totalMem = (Number) this.conf.get(Config.STORM_WORKER_CGROUP_MEMORY_MB_LIMIT);
        } else if (resourcesMap.get("memory") != null) {
            totalMem = (Number) resourcesMap.get("memory");
        }

        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        try {
            this.center.createCgroup(workerGroup);
        } catch (Exception e) {
            throw new RuntimeException("Error when creating Cgroup! Exception: ", e);
        }

        if (cpuNum != null) {
            CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);
            try {
                cpuCore.setCpuShares(cpuNum.intValue());
            } catch (IOException e) {
                throw new RuntimeException("Cannot set cpu.shares! Exception: ", e);
            }
        }

        // TEMPORARY CHECK TO DEAL WITH KERNEL BUGS
        if ((boolean)this.conf.get(Config.STORM_CGROUP_MEMORY_ENFORCEMENT_ENABLE)) {
            if (totalMem != null) {
                int cGroupMem = (int) (Math.ceil((double) this.conf.get(Config.STORM_CGROUP_MEMORY_LIMIT_TOLERANCE_MARGIN_MB)));
                long memLimit = Long.valueOf((totalMem.longValue() + cGroupMem)* 1024 * 1024);
                MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
                try {
                    memCore.setPhysicalUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.limit_in_bytes! Exception: ", e);
                }
                // need to set memory.memsw.limit_in_bytes after setting memory.limit_in_bytes or error might occur
                try {
                    memCore.setWithSwapUsageLimit(memLimit);
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set memory.memsw.limit_in_bytes! Exception: ", e);
                }
            }
        }

        if (Utils.getBoolean(this.conf.get(Config.STORM_CGROUP_INHERIT_CPUSET_CONFIGS), false)) {
            if (workerGroup.getParent().getCores().containsKey(SubSystemType.cpuset)) {
                CpusetCore parentCpusetCore = (CpusetCore) workerGroup.getParent().getCores().get(SubSystemType.cpuset);
                CpusetCore cpusetCore = (CpusetCore) workerGroup.getCores().get(SubSystemType.cpuset);
                try {
                    cpusetCore.setCpus(parentCpusetCore.getCpus());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set cpuset.cpus! Exception: ", e);
                }
                try {
                    cpusetCore.setMems(parentCpusetCore.getMems());
                } catch (IOException e) {
                    throw new RuntimeException("Cannot set cpuset.mems! Exception: ", e);
                }
            }
        }
        //update which cgroups to monitor
        if (Utils.getBoolean(conf.get(Config.STORM_CGROUP_OOM_MONITORING_ENABLE), false)) {
            this.oomMonitor.addCgroupToMonitor(workerGroup);
        }
    }

    @Override
    public void releaseResourcesForWorker(String workerId) {
        LOG.info("Cleaning up cgroups for worker {}", workerId);
        CgroupCommon workerGroup = new CgroupCommon(workerId, hierarchy, this.rootCgroup);

        if (!Utils.CheckDirExists(workerGroup.getDir())) {
            LOG.info("Nothing to cleanup for cgroups for worker {}", workerId);
            return;
        }

        try {
            Set<Integer> tasks = workerGroup.getTasks();
            if (!tasks.isEmpty()) {
                throw new Exception("Cannot correctly showdown worker CGroup " + workerId + " tasks " + tasks.toString() + " still running!");
            }
            //update which cgroups to monitor
            if (Utils.getBoolean(conf.get(Config.STORM_CGROUP_OOM_MONITORING_ENABLE), false)) {
                this.oomMonitor.removeCgroupToMonitor(workerGroup);
            }
            this.center.deleteCgroup(workerGroup);
        } catch (Exception e) {
            LOG.error("Exception thrown when shutting worker {} Exception: ", workerId, e);
            throw new RuntimeException("Unsuccessful in deleting CGroup " + workerId);
        }
    }

    @Override
    public List<String> getLaunchCommand(String workerId, List<String> existingCommand) {
        List<String> newCommand = getLaunchCommandPrefix(workerId);
        newCommand.addAll(existingCommand);
        return newCommand;
    }

    @Override
    public List<String> getLaunchCommandPrefix(String workerId) {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);

        if (!this.rootCgroup.getChildren().contains(workerGroup)) {
            throw new RuntimeException("cgroup " + workerGroup + " doesn't exist! Need to reserve resources for worker first!");
        }

        StringBuilder sb = new StringBuilder();

        sb.append(this.conf.get(Config.STORM_CGROUP_CGEXEC_CMD)).append(" -g ");

        Iterator<SubSystemType> it = this.hierarchy.getSubSystems().iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
            if (it.hasNext()) {
                sb.append(",");
            } else {
                sb.append(":");
            }
        }
        sb.append("/").append(workerGroup.getParent().getName()).append("/").append(workerGroup.getName());
        List<String> newCommand = new ArrayList<String>();
        newCommand.addAll(Arrays.asList(sb.toString().split(" ")));
        return newCommand;
    }

    @Override
    public Set<Long> getRunningPIDs(String workerId) throws IOException {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        if (!this.rootCgroup.getChildren().contains(workerGroup)) {
            LOG.warn("cgroup {} doesn't exist!", workerGroup);
            return Collections.emptySet();
        }
        return workerGroup.getPids();
    }

    @Override
    public long getMemoryUsage(String workerId) throws IOException {
        CgroupCommon workerGroup = new CgroupCommon(workerId, this.hierarchy, this.rootCgroup);
        MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
        return memCore.getPhysicalUsage();
    }

    private static final Pattern MEMINFO_PATTERN = Pattern.compile("^([^:\\s]+):\\s*([0-9]+)\\s*kB$");
    
    static long getMemInfoFreeMB() throws IOException {
        //MemFree:        14367072 kB
        //Buffers:          536512 kB
        //Cached:          1192096 kB
        // MemFree + Buffers + Cached
        long memFree = 0;
        long buffers = 0;
        long cached = 0;
        try (BufferedReader in = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line = null;
            while((line = in.readLine()) != null) {
                Matcher m = MEMINFO_PATTERN.matcher(line);
                if (m.matches()) {
                    String tag = m.group(1);
                    if (tag.equalsIgnoreCase("MemFree")) {
                        memFree = Long.parseLong(m.group(2));
                    } else if (tag.equalsIgnoreCase("Buffers")) {
                        buffers = Long.parseLong(m.group(2));
                    } else if (tag.equalsIgnoreCase("Cached")) {
                        cached = Long.parseLong(m.group(2));
                    }
                }
            }
        }
        return (memFree + buffers + cached) / 1024;
    }
    
    @Override
    public long getSystemFreeMemoryMB() throws IOException {
        long rootCgroupLimitFree = Long.MAX_VALUE;
        try {
            MemoryCore memRoot = (MemoryCore) rootCgroup.getCores().get(SubSystemType.memory);
            if (memRoot != null) {
                //For cgroups no limit is max long.
                long limit = memRoot.getPhysicalUsageLimit();
                long used = memRoot.getMaxPhysicalUsage();
                rootCgroupLimitFree = (limit - used)/1024/1024;
            }
        } catch (FileNotFoundException e) {
            //Ignored if cgroups is not setup don't do anything with it
        }

        return Long.min(rootCgroupLimitFree, getMemInfoFreeMB());
    }
}
