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
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.scheduler.resource.ResourceAwareScheduler;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy;
import backtype.storm.utils.Utils;

import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static backtype.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.*;

public class TestRASConstraintSolver {
    private static final Logger LOG = LoggerFactory.getLogger(TestRASConstraintSolver.class);
    private static final int NUM_SUPS = 20;
    private static final int NUM_WORKERS_PER_SUP = 4;
    private static final int MAX_TRAVERSAL_DEPTH = 1000000;
    private static final int NUM_WORKERS = NUM_SUPS * NUM_WORKERS_PER_SUP;
    
    @Test
    public void testConstraintSolver() {
        Map<String, SupervisorDetails> supMap = genSupervisors(30, 16, 400, 1024 * 4);

        ConstraintSolverStrategy cs = new ConstraintSolverStrategy();

        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-1", "bolt-1", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        List<String> spread = new LinkedList<>();
        spread.add("spout-0");

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
        config.put(Config.TOPOLOGY_CONSTRAINTS, constraints);
        config.put(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        TopologyDetails topo = genTopology("testTopo", config, 2, 3, 30, 300, 0, 0, "user");
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(new INimbusTest(), supMap, new HashMap<>(), topologies, new Config());
        cs.prepare(config);

        SchedulingResult result = cs.schedule(cluster, topo);
        
        Assert.assertTrue("Assert scheduling topology success", result.isSuccess());
        Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
        Assert.assertTrue("Valid Scheduling?", ConstraintSolverStrategy.validateSolution(cluster, topo));

        //simulate worker loss
        Map<ExecutorDetails, WorkerSlot> newExecToSlot = new HashMap<>();
        SchedulerAssignment assignment = cluster.getAssignmentById(topo.getId());

        Set<WorkerSlot> slotsToDelete = new HashSet<>();
        Set<WorkerSlot> slots = assignment.getSlots();
        int i = 0;
        for (WorkerSlot slot: slots) {
            if (i % 5 == 0) {
                slotsToDelete.add(slot);
            }
            i++;
        }

        LOG.info("\n\n\t\tKILL WORKER(s) {}", slotsToDelete);
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry: assignment.getExecutorToSlot().entrySet()) {
            if (!slotsToDelete.contains(entry.getValue())) {
                newExecToSlot.put(entry.getKey(), entry.getValue());
            }
        }

        Map<String, SchedulerAssignment> newAssignments = new HashMap<>();
        newAssignments.put(topo.getId(), new SchedulerAssignmentImpl(topo.getId(), newExecToSlot, null, null));
        cluster.setAssignments(newAssignments, false);

        cs = new ConstraintSolverStrategy();
        cs.prepare(config);
        LOG.info("\n\n\t\tScheduling again...");
        result = cs.schedule(cluster, topo);
        LOG.info("\n\n\t\tDone scheduling...");

        Assert.assertTrue("Assert scheduling topology success", result.isSuccess());
        Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
        Assert.assertTrue("Valid Scheduling?", ConstraintSolverStrategy.validateSolution(cluster, topo));
    }

    @Test
    public void testIntegrationWithRAS() {
        Map<String, SupervisorDetails> supMap = genSupervisors(30, 16, 400, 1024 * 4);

        List<List<String>> constraints = new LinkedList<>();
        addContraints("spout-0", "bolt-0", constraints);
        addContraints("bolt-1", "bolt-1", constraints);
        addContraints("bolt-1", "bolt-2", constraints);
        List<String> spread = new LinkedList<>();
        spread.add("spout-0");

        Map<String, Object> config = Utils.readDefaultConfig();
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, ConstraintSolverStrategy.class.getName());
        config.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spread);
        config.put(Config.TOPOLOGY_CONSTRAINTS, constraints);
        config.put(Config.TOPOLOGY_CONSTRAINTS_MAX_DEPTH_TRAVERSAL, MAX_TRAVERSAL_DEPTH);
        config.put(Config.TOPOLOGY_WORKERS, NUM_WORKERS);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 100000);
        config.put(Config.TOPOLOGY_PRIORITY, 1);
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 100);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);

        TopologyDetails topo = genTopology("testTopo", config, 2, 3, 30, 300, 0, 0, "user");
        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);
        Topologies topologies = new Topologies(topoMap);
        Cluster cluster = new Cluster(new INimbusTest(), supMap, new HashMap<String, SchedulerAssignmentImpl>(), topologies, config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        rs.prepare(config);
        rs.schedule(topologies, cluster);

        assertStatusSuccess(cluster, topo.getId());
        Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());

        //simulate worker loss
        Map<ExecutorDetails, WorkerSlot> newExecToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
        Map<ExecutorDetails, WorkerSlot> execToSlot = cluster.getAssignmentById(topo.getId()).getExecutorToSlot();
        Iterator<Map.Entry<ExecutorDetails, WorkerSlot>> it =execToSlot.entrySet().iterator();
        for (int i = 0; i<execToSlot.size()/2; i++) {
            ExecutorDetails exec = it.next().getKey();
            WorkerSlot ws = it.next().getValue();
            newExecToSlot.put(exec, ws);
        }
        Map<String, SchedulerAssignment> newAssignments = new HashMap<String, SchedulerAssignment>();
        newAssignments.put(topo.getId(), new SchedulerAssignmentImpl(topo.getId(), newExecToSlot, null, null));
        cluster.setAssignments(newAssignments, false);
        
        rs.prepare(config);
        rs.schedule(topologies, cluster);

        assertStatusSuccess(cluster, topo.getId());
        Assert.assertEquals("topo all executors scheduled?", 0, cluster.getUnassignedExecutors(topo).size());
    }

    public static void addContraints(String comp1, String comp2, List<List<String>> constraints) {
        LinkedList<String> constraintPair = new LinkedList<String>();
        constraintPair.add(comp1);
        constraintPair.add(comp2);
        constraints.add(constraintPair);
    }

}
