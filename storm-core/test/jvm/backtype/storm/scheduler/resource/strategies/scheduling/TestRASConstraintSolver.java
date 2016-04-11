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

package backtype.storm.scheduler.resource.strategies.scheduling;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.ResourceAwareScheduler;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.SchedulingState;
import backtype.storm.scheduler.resource.TestUtilsForResourceAwareScheduler;
import backtype.storm.scheduler.resource.User;
import backtype.storm.utils.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestRASConstraintSolver {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestRASConstraintSolver.class);
    private static final int NUM_SUPS = 20;
    private static final int NUM_WORKERS_PER_SUP = 4;
    private static final int MAX_TRAVERSAL_DEPTH = 1000000;
    private static final int NUM_WORKERS = NUM_SUPS * NUM_WORKERS_PER_SUP;
    private final String TOPOLOGY_SUBMITTER = "jerry";

    @Test
    public void testConstraintSolver() {
        Map<String, Double> total_resources = new HashMap<String, Double>();
        total_resources.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        total_resources.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 4);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(30, 16, total_resources);

        ConstraintSolverStrategy cs = new ConstraintSolverStrategy();

        Cluster cluster = new Cluster(new TestUtilsForResourceAwareScheduler.INimbusTest(), supMap, new HashMap<String, SchedulerAssignmentImpl>(), new Config());

        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-1", "bolt-1", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        List<String> spread = new LinkedList<String>();
        spread.add("spout-0");

        Map config = Utils.readDefaultConfig();
        config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
        config.put(Config.TOPOLOGY_CONSTRAINTS, constraints);
        config.put(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_SUBMITTER_USER, TOPOLOGY_SUBMITTER);
        config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        TopologyDetails topo = TestUtilsForResourceAwareScheduler.getTopology("testTopo", config, 2, 3, 30, 300, 0, 0);
        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);

        cs.prepare(new SchedulingState(new HashMap<String, User>(), cluster, topologies, config));

        SchedulingResult result = cs.schedule(topo);

        Assert.assertTrue("topo get scheduled sucessfully", result.isSuccess());

        Assert.assertTrue("Valid Scheduling?", cs.validateSolution(TestUtilsForResourceAwareScheduler.getExecToWorkerResultMapping(result.getSchedulingResultMap())));
    }

    @Test
    public void testIntegrationWithRAS() {
        Map<String, Double> total_resources = new HashMap<String, Double>();
        total_resources.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        total_resources.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 4);
        Map<String, SupervisorDetails> supMap = TestUtilsForResourceAwareScheduler.genSupervisors(30, 16, total_resources);

        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-1", "bolt-1", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        List<String> spread = new LinkedList<String>();
        spread.add("spout-0");

        Map config = Utils.readDefaultConfig();
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, backtype.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, backtype.storm.scheduler.resource.strategies.scheduling.ConstraintSolverStrategy.class.getName());
        config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
        config.put(Config.TOPOLOGY_CONSTRAINTS, constraints);
        config.put(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_SUBMITTER_USER, TOPOLOGY_SUBMITTER);
        config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        Cluster cluster = new Cluster(new TestUtilsForResourceAwareScheduler.INimbusTest(), supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        TopologyDetails topo = TestUtilsForResourceAwareScheduler.getTopology("testTopo", config, 2, 3, 30, 300, 0, 0);
        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);

        Assert.assertTrue("Assert scheduling topology success", TestUtilsForResourceAwareScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
    }

    public static void addContraints(String comp1, String comp2, List<List<String>> constraints) {
        LinkedList<String> constraintPair = new LinkedList<String>();
        constraintPair.add(comp1);
        constraintPair.add(comp2);
        constraints.add(constraintPair);
    }
}
