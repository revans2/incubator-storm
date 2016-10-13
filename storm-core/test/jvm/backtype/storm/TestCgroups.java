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

package backtype.storm;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.cluster.IStormClusterState;

import org.apache.storm.container.cgroup.CgroupCenter;
import org.apache.storm.container.cgroup.CgroupCommon;
import org.apache.storm.container.cgroup.Hierarchy;
import org.apache.storm.container.cgroup.SubSystemType;
import org.apache.storm.container.cgroup.core.CpuCore;
import org.apache.storm.container.cgroup.core.MemoryCore;
import org.apache.storm.container.cgroup.monitor.CgroupOOMMonitor;
import org.junit.Assert;
import org.junit.Assume;
import org.apache.storm.container.cgroup.CgroupManager;
import backtype.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for CGroups
 */

public class TestCgroups {

    private static final Logger LOG = LoggerFactory.getLogger(TestCgroups.class);

    /**
     * Test whether cgroups are setup up correctly for use.  Also tests whether Cgroups produces the right command to
     * start a worker and cleans up correctly after the worker is shutdown
     */
    @Test
    public void testSetupAndTearDown() throws IOException {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        //We don't want to run the test is CGroups are not setup
        Assume.assumeTrue("Check if CGroups are setup", ((boolean) config.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE)));

        Assert.assertTrue("Check if STORM_CGROUP_HIERARCHY_DIR exists", stormCgroupHierarchyExists(config));
        Assert.assertTrue("Check if STORM_SUPERVISOR_CGROUP_ROOTDIR exists", stormCgroupSupervisorRootDirExists(config));

        //set to false since we don't want to test this feature in the test
        config.put(Config.STORM_CGROUP_OOM_MONITORING_ENABLE, false);

        CgroupManager manager = new CgroupManager();
        manager.prepare(config);

        Map<String, Number> resourcesMap = new HashMap<>();
        resourcesMap.put("cpu", 200);
        resourcesMap.put("memory", 1024);
        String workerId = UUID.randomUUID().toString();
        manager.reserveResourcesForWorker(workerId, resourcesMap);

        List<String> commandList = manager.getLaunchCommand(workerId, new ArrayList<String>());
        StringBuilder command = new StringBuilder();
        for (String entry : commandList) {
            command.append(entry).append(" ");
        }

        List<String> cgroupSubsystems = Arrays.asList(command.toString().replaceAll("/bin/cgexec -g ", "").replaceAll(":/.*", "").split(","));
        List<String> userListSubsystems = (List<String>) config.get(Config.STORM_CGROUP_RESOURCES);

        Assert.assertTrue("The list of subsystems declared by the user to use is included", cgroupSubsystems.containsAll(userListSubsystems));

        String correctCommand = config.get(Config.STORM_CGROUP_CGEXEC_CMD) + " -g " + StringUtils.join(cgroupSubsystems, ',') +  ":/"
                + config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId + " ";

        Assert.assertTrue("Check if cgroup launch command: '" + command + "' is correct. Correct commands '" + correctCommand + "'",
                command.toString().equals(correctCommand));

        String pathToWorkerCgroupDir = ((String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR))
                + "/" + ((String) config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR)) + "/" + workerId;

        Assert.assertTrue("Check if cgroup directory exists for worker", dirExists(pathToWorkerCgroupDir));

        /* validate cpu settings */

        String pathToCpuShares = pathToWorkerCgroupDir + "/cpu.shares";
        Assert.assertTrue("Check if cpu.shares file exists", fileExists(pathToCpuShares));
        Assert.assertEquals("Check if the correct value is written into cpu.shares", "200", readFileAll(pathToCpuShares));

        /* validate memory settings */

        String pathTomemoryLimitInBytes = pathToWorkerCgroupDir + "/memory.limit_in_bytes";

        Assert.assertTrue("Check if memory.limit_in_bytes file exists", fileExists(pathTomemoryLimitInBytes));
        Assert.assertEquals("Check if the correct value is written into memory.limit_in_bytes", String.valueOf(1024 * 1024 * 1024), readFileAll(pathTomemoryLimitInBytes));

        manager.releaseResourcesForWorker(workerId);

        Assert.assertFalse("Make sure cgroup was removed properly", dirExists(pathToWorkerCgroupDir));
    }

    /**
     * Test if cgroup OOM notification is working correctly
     * NOTE: remember to set java.library.path or LD_LIBRARY_PATH to include the path to libcgroupOOMMonitor.so native library
     */
    @Test
    public void testCgroupMonitor() throws IOException, InterruptedException {
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        //We don't want to run the test is CGroups are not setup
        Assume.assumeTrue("Check if CGroups are setup", ((boolean) config.get(Config.STORM_RESOURCE_ISOLATION_PLUGIN_ENABLE)));
        Assume.assumeTrue("Check if Cgroup oom monitoring is enabled", (boolean) config.get(Config.STORM_CGROUP_OOM_MONITORING_ENABLE));
        Assume.assumeTrue("check if stress utility exists", fileExists("/usr/bin/stress"));

        Assert.assertTrue("Check if STORM_CGROUP_HIERARCHY_DIR exists", stormCgroupHierarchyExists(config));
        Assert.assertTrue("Check if STORM_SUPERVISOR_CGROUP_ROOTDIR exists", stormCgroupSupervisorRootDirExists(config));

        CgroupCenter center = CgroupCenter.getInstance();
        List<SubSystemType> subSystemTypes = new LinkedList<>();
        for (String resource : Config.getCgroupStormResources(config)) {
            subSystemTypes.add(SubSystemType.getSubSystem(resource));
        }
        Hierarchy hierarchy = center.getHierarchyWithSubSystems(subSystemTypes);
        String rootDir = Config.getCgroupRootDir(config);
        CgroupCommon rootCgroup = new CgroupCommon(rootDir, hierarchy, hierarchy.getRootCgroups());

        // creating cgroup
        String workerId = UUID.randomUUID().toString();
        CgroupCommon workerGroup = new CgroupCommon(workerId, hierarchy, rootCgroup);
        center.createCgroup(workerGroup);

        // setting cpu and mem
        CpuCore cpuCore = (CpuCore) workerGroup.getCores().get(SubSystemType.cpu);
        cpuCore.setCpuShares(10);
        MemoryCore memCore = (MemoryCore) workerGroup.getCores().get(SubSystemType.memory);
        long memoryLimit = Long.valueOf(128 * 1024 * 1024);
        memCore.setPhysicalUsageLimit(memoryLimit);
        memCore.setWithSwapUsageLimit(memoryLimit);

        IStormClusterState stormClusterState = mock(IStormClusterState.class);

        CgroupOOMMonitor oomMonitor = new CgroupOOMMonitor(config, stormClusterState);
        oomMonitor.addCgroupToMonitor(workerGroup);

        Assert.assertEquals("Check right number of cgroups being monitored", 1, oomMonitor.getCgroupsBeingMonitored().size());
        Assert.assertEquals("Check right cgroup being monitored", workerId, oomMonitor.getCgroupsBeingMonitored().iterator().next());

        List<String> userListSubsystems = (List<String>) config.get(Config.STORM_CGROUP_RESOURCES);

        String command = config.get(Config.STORM_CGROUP_CGEXEC_CMD) + " -g " + StringUtils.join(userListSubsystems, ',') + ":/"
                + config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR) + "/" + workerId + " /usr/bin/stress --vm 1";

        String output = executeCommand(command);
        String notification = null;
        //wait for oom notification
        for (int i = 0; i < 10; i++) {
            notification = oomMonitor.getNextNotification();
            if (notification != null) {
                break;
            }
            Utils.sleep(1000);
        }
        Assert.assertNotNull("Check if there is a notification", notification);
        Assert.assertEquals("Check if right cgroup got an oom error", workerId, notification);

        //deleting cgroup
        oomMonitor.removeCgroupToMonitor(workerGroup);
        center.deleteCgroup(workerGroup);

        Assert.assertEquals("Making sure not cgroups are still being monitored after cleanup", 0, oomMonitor.getCgroupsBeingMonitored().size());
    }

    private String executeCommand(String command) throws IOException, InterruptedException {

        StringBuffer output = new StringBuffer();
        Process p;
        p = Runtime.getRuntime().exec(command);
        p.waitFor();
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(p.getInputStream()));

        String line = "";
        while ((line = reader.readLine()) != null) {
            output.append(line + "\n");
        }
        return output.toString();
    }


    private boolean stormCgroupHierarchyExists(Map config) {
        String pathToStormCgroupHierarchy = (String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR);
        return dirExists(pathToStormCgroupHierarchy);
    }

    private boolean stormCgroupSupervisorRootDirExists(Map config) {
        String pathTostormCgroupSupervisorRootDir = ((String) config.get(Config.STORM_CGROUP_HIERARCHY_DIR))
                + "/" + ((String) config.get(Config.STORM_SUPERVISOR_CGROUP_ROOTDIR));

        return dirExists(pathTostormCgroupSupervisorRootDir);
    }

    private boolean dirExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && path.isDirectory();
    }

    private boolean fileExists(String rawPath) {
        File path = new File(rawPath);
        return path.exists() && !path.isDirectory();
    }

    private String readFileAll(String filePath) throws IOException {
        byte[] data = Files.readAllBytes(Paths.get(filePath));
        return new String(data).trim();
    }
}
