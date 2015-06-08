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
package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.resource.RAS_Component;
import backtype.storm.scheduler.resource.RAS_TYPES;

import backtype.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyDetails {
    String topologyId;
    Map topologyConf;
    StormTopology topology;
    Map<ExecutorDetails, String> executorToComponent;
    int numWorkers;
    //<ExecutorDetails - Task, Map<String - Type of resource, Map<String - type of that resource, Double - amount>>>
    private Map<ExecutorDetails, Map<String, Map<String, Double>>> _resourceList;

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDetails.class);

    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, int numWorkers) {
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.numWorkers = numWorkers;
    }

    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, int numWorkers, Map<ExecutorDetails, String> executorToComponents) {
        this(topologyId, topologyConf, topology, numWorkers);
        this.executorToComponent = new HashMap<ExecutorDetails, String>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
        this.initResourceList();
    }

    public String getId() {
        return topologyId;
    }

    public String getName() {
        return (String) this.topologyConf.get(Config.TOPOLOGY_NAME);
    }

    public Map getConf() {
        return topologyConf;
    }

    public int getNumWorkers() {
        return numWorkers;
    }

    public Map<ExecutorDetails, String> getExecutorToComponent() {
        return this.executorToComponent;
    }

    public Map<ExecutorDetails, String> selectExecutorToComponent(Collection<ExecutorDetails> executors) {
        Map<ExecutorDetails, String> ret = new HashMap<ExecutorDetails, String>(executors.size());
        for (ExecutorDetails executor : executors) {
            String compId = this.executorToComponent.get(executor);
            if (compId != null) {
                ret.put(executor, compId);
            }
        }

        return ret;
    }

    public Collection<ExecutorDetails> getExecutors() {
        return this.executorToComponent.keySet();
    }

    private void initResourceList() {
        _resourceList = new HashMap<ExecutorDetails, Map<String, Map<String, Double>>>();
        // Extract bolt memory info
        if (this.topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : this.topology.get_bolts().entrySet()) {
                //the json_conf is populated by TopologyBuilder (e.g. bolt.set_memory_load)
                Map<String, Map<String, Double>> topology_resources = this.parseResources(bolt
                        .getValue().get_common().get_json_conf());
                LOG.info("Bolt" + bolt);
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent : executorToComponent.entrySet()) {
                    if (bolt.getKey().equals(anExecutorToComponent.getValue())) {
                        this.checkIntialization(topology_resources, anExecutorToComponent.getKey(), bolt.getValue().toString());
                        _resourceList.put(anExecutorToComponent.getKey(), topology_resources);
                    }
                }
            }
        } else {
            LOG.warn("Topology " + topologyId + " does not seem to have any bolts!");
        }
        // Extract spout memory info
        if (this.topology.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spout : this.topology.get_spouts().entrySet()) {
                Map<String, Map<String, Double>> topology_resources = this.parseResources(spout
                        .getValue().get_common().get_json_conf());
                LOG.info("Spout" + spout);
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent : executorToComponent.entrySet()) {
                    if (spout.getKey().equals(anExecutorToComponent.getValue())) {
                        this.checkIntialization(topology_resources, anExecutorToComponent.getKey(), spout.getValue().toString());
                        _resourceList.put(anExecutorToComponent.getKey(), topology_resources);
                    }
                }
            }
        } else {
            LOG.warn("Topology " + topologyId + " does not seem to have any spouts!");
        }
    }


    /* Return type is a map of:
     *  Map<String, Map<String, Double>>
     *  First String can be: RAS_Globals.TYPE_CPU   |  RAS_Globals.TYPE_MEMORY
     *  If First String == RAS_Globals.TYPE_CPU
     *      Second string = RAS_Globals.TYPE_CPU_TOTAL, a single "Double" value parsed from JSON from Config.TOPOLOGY_RESOURCES_CPU
     *      Therefore the inner map in this case will have only one member
     *  If First String == RAS_Globals.TYPE_MEMORY
     *      Map<String, Double> parsed from JSON from Config.TOPOLOGY_RESOURCES_MEMORY_MB
     *      The inner map can have more than one member. Possibly on-heap and off-heap, etc.
     */
    private Map<String, Map<String, Double>> parseResources(String input) {
        Map<String, Map<String, Double>> topology_resources =
                new HashMap<String, Map<String, Double>>();
        JSONParser parser = new JSONParser();
        LOG.info("Input" + input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;
                Double topoMemOnHeap =
                        (Double) jsonObject.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB);
                Double topoMemOffHeap =
                        (Double) jsonObject.get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB);
                Double topoCPU =
                        (Double) jsonObject.get(Config.TOPOLOGY_RESOURCES_CPU);
                LOG.debug("MemOnHeap" + topoMemOnHeap);
                LOG.debug("MemOffHeap" + topoMemOffHeap);
                if (topoMemOnHeap != null) {
                    topology_resources.put((String) topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY), new HashMap<String, Double>());
                    topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY)).put(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (topoMemOffHeap != null) {
                    topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY)).put(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (topoCPU != null) {
                    topology_resources.put((String)topologyConf.get(Config.TOPOLOGY_TYPE_CPU), new HashMap<String, Double>());
                    topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_CPU)).put((String)topologyConf.get(Config.TOPOLOGY_TYPE_CPU_TOTAL), topoCPU);
                }
                LOG.debug("Topology Resources" + topology_resources);
            }
        } catch (ParseException e) {
            LOG.error(e.toString());
            return null;
        }
        return topology_resources;
    }

    private void checkIntialization(Map<String, Map<String, Double>> topology_resources,
                                    ExecutorDetails exec, String Com) {
        this.checkInitMem(topology_resources, exec, Com);
        LOG.debug("Topology Resources in parseResources after checkInit" + topology_resources);
        this.checkInitCPU(topology_resources, exec, Com);
        LOG.debug("Topology Resources in parseResources after checkCPU" + topology_resources);
    }

    private void checkInitMem(Map<String, Map<String, Double>> topology_resources,
                              ExecutorDetails exec, String Com) {

        String msg = "";
        if (topology_resources.size() == 0 || !topology_resources.containsKey(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY))) {
            topology_resources.put((String) topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY), new HashMap<String, Double>());
        }
        if (!topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY)).containsKey(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB)) {
            topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY))
                    .put(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB, (Double) topologyConf.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB));
            msg +=
                    "Resource : " + topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY) +
                            " Type: " + topologyConf.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB) +
                            " set to default " + topologyConf.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB).toString() +
                            "\n";
        }
        if (!topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY)).containsKey(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB)) {
            topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY))
                    .put(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB, (Double) topologyConf.get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB));
            msg +=
                    "Resource : " + topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY) +
                            " Type: " + topologyConf.get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB) +
                            " set to default " + topologyConf.get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB).toString() +
                            "\n";
        }
        if (msg != "") {
            LOG.debug(
                    "Unable to extract resource requirement of Executor " +
                            exec + " for Component " + Com + "\n" + msg);
        }
    }

    private void checkInitCPU(Map<String, Map<String, Double>> topology_resources,
                              ExecutorDetails exec, String Com) {

        String msg = "";
        if (topology_resources.size() == 0 || !topology_resources.containsKey(topologyConf.get(Config.TOPOLOGY_TYPE_CPU))) {
            topology_resources.put((String)topologyConf.get(Config.TOPOLOGY_TYPE_CPU), new HashMap<String, Double>());
        }
        if (!topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_CPU)).containsKey(topologyConf.get(Config.TOPOLOGY_TYPE_CPU_TOTAL))) {
            topology_resources.get(topologyConf.get(Config.TOPOLOGY_TYPE_CPU))
                    .put((String) topologyConf.get(Config.TOPOLOGY_TYPE_CPU_TOTAL), (Double) topologyConf.get(Config.TOPOLOGY_DEFAULT_CPU_REQUIREMENT));
            msg +=
                    "Resource : " + topologyConf.get(Config.TOPOLOGY_TYPE_CPU) +
                            " Type: " + topologyConf.get(Config.TOPOLOGY_TYPE_CPU_TOTAL) +
                            " set to default " + topologyConf.get(Config.TOPOLOGY_DEFAULT_CPU_REQUIREMENT).toString() +
                            "\n";
        }
        if (!msg.equals("")) {
            LOG.debug(
                    "Unable to extract resource requirement of Executor " +
                            exec + " for Component " + Com + "\n" + msg);
        }
    }
    private List<ExecutorDetails> componentToExecs(String comp) {
        List<ExecutorDetails> execs = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, String> entry : executorToComponent.entrySet()) {
            if (entry.getValue().equals(comp)) {
                execs.add(entry.getKey());
            }
        }
        return execs;
    }


    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     *  Returns a representation of the non-system components of the topology graph
     *  Each RAS_Component object in the returning map is populated with the list of its
     *  parents, children and execs assigned to that component.
     */
    public Map<String, RAS_Component> getRAS_Components() {
        Map<String, RAS_Component> all_comp = new HashMap<String, RAS_Component>();

        StormTopology storm_topo = this.topology;
        // spouts
        for (Map.Entry<String, SpoutSpec> spoutEntry : storm_topo
                .get_spouts().entrySet()) {
            if (!Utils.isSystemId(spoutEntry.getKey())) {
                RAS_Component newComp = null;
                if (all_comp.containsKey(spoutEntry.getKey())) {
                    newComp = all_comp.get(spoutEntry.getKey());
                } else {
                    newComp = new RAS_Component(spoutEntry.getKey());
                    newComp.execs = componentToExecs(newComp.id);
                    all_comp.put(spoutEntry.getKey(), newComp);
                }
                newComp.type = RAS_Component.ComponentType.SPOUT;


                for (Map.Entry<GlobalStreamId, Grouping> spoutInput : spoutEntry
                        .getValue().get_common().get_inputs()
                        .entrySet()) {

                    if (!Utils.isSystemId(spoutInput.getKey().get_componentId())) {
                        newComp.parents.add(spoutInput.getKey()
                                .get_componentId());
                        if (!all_comp.containsKey(spoutInput
                                .getKey().get_componentId())) {
                            all_comp.put(spoutInput.getKey()
                                            .get_componentId(),
                                    new RAS_Component(spoutInput.getKey()
                                            .get_componentId()));
                        }
                        all_comp.get(spoutInput.getKey()
                                .get_componentId()).children.add(spoutEntry
                                .getKey());
                    }
                }

            }
        }
        // bolts
        for (Map.Entry<String, Bolt> boltEntry : storm_topo.get_bolts()
                .entrySet()) {
            if (!Utils.isSystemId(boltEntry.getKey())) {
                RAS_Component newComp = null;
                if (all_comp.containsKey(boltEntry.getKey())) {
                    newComp = all_comp.get(boltEntry.getKey());
                } else {
                    newComp = new RAS_Component(boltEntry.getKey());
                    newComp.execs = componentToExecs(newComp.id);
                    all_comp.put(boltEntry.getKey(), newComp);
                }
                newComp.type = RAS_Component.ComponentType.BOLT;

                for (Map.Entry<GlobalStreamId, Grouping> boltInput : boltEntry
                        .getValue().get_common().get_inputs()
                        .entrySet()) {
                    if (!Utils.isSystemId(boltInput.getKey().get_componentId())) {
                        newComp.parents.add(boltInput.getKey()
                                .get_componentId());
                        if (!all_comp.containsKey(boltInput
                                .getKey().get_componentId())) {
                            all_comp.put(boltInput.getKey()
                                            .get_componentId(),
                                    new RAS_Component(boltInput.getKey()
                                            .get_componentId()));
                        }
                        all_comp.get(boltInput.getKey()
                                .get_componentId()).children.add(boltEntry
                                .getKey());
                    }
                }
            }
        }
        return all_comp;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * Gets the on heap memory requirement for a
     * certain task within a topology
     * @param exec the executor the inquiry is concerning.
     * @return Double the amount of on heap memory
     * requirement for this exec in topology topoId.
     */
    public Double getOnHeapMemoryRequirement(ExecutorDetails exec) {
        Double ret = null;
        if (hasExecInTopo(exec)) {
            ret = _resourceList
                    .get(exec).get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY))
                    .get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB);
            if (ret == null) {
                LOG.error("{} not set!" + topologyConf.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB));
            }
        }
        return ret;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * Gets the off heap memory requirement for a
     * certain task within a topology
     * @param exec the executor the inquiry is concerning.
     * @return Double the amount of off heap memory
     * requirement for this exec in topology topoId.
     */
    public Double getOffHeapMemoryRequirement(ExecutorDetails exec) {
        Double ret = null;
        if (hasExecInTopo(exec)) {
            LOG.debug("ResourceList" + _resourceList);
            ret = _resourceList
                    .get(exec).get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY))
                    .get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB);
            if (ret == null) {
                LOG.error("{} not set!" + Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB);
            }
        }
        return ret;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * Gets the total memory requirement for a task
     * @param exec the executor the inquiry is concerning.
     * @return Double the total memory requirement
     *  for this exec in topology topoId.
     */
    public Double getTotalMemReqTask(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return getOffHeapMemoryRequirement(exec)
                    + getOnHeapMemoryRequirement(exec);
        }
        LOG.info("cannot find {}", exec);
        return null;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * Gets the total memory resource list for a
     * set of tasks that is part of a topology.
     * @return Map<ExecutorDetails, Double> a map of the total memory requirement
     *  for all tasks in topology topoId.
     */
    public Map<ExecutorDetails, Double> getTotalMemoryResourceList() {
        Map<ExecutorDetails, Double> ret = new HashMap<ExecutorDetails, Double>();
        for (ExecutorDetails exec : _resourceList.keySet()) {
            ret.put(exec, getTotalMemReqTask(exec));
        }
        return ret;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public Double getTotalCpuReqTask(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return _resourceList.get(exec).get(topologyConf.get(Config.TOPOLOGY_TYPE_CPU)).get(topologyConf.get(Config.TOPOLOGY_TYPE_CPU_TOTAL));
        }
        LOG.info("cannot find - {}", exec);
        return null;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public Map<String, Map<String, Double>> getTaskResourceReqList(ExecutorDetails exec) {
        if (hasExecInTopo(exec)) {
            return _resourceList.get(exec);
        }
        LOG.info("cannot find - {}", exec);
        return null;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     *
     * @return Boolean whether or not a certain ExecutorDetail is included in the _resourceList.
     */
    public boolean hasExecInTopo(ExecutorDetails exec) {
        return _resourceList.containsKey(exec);
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public void addResourcesForExec(ExecutorDetails exec, Map<String, Map<String, Double>> resourceList) {
        if (hasExecInTopo(exec)) {
            LOG.warn("Executor {} already exists...ResourceList: {}", exec, getTaskResourceReqList(exec));
            return;
        }
        _resourceList.put(exec, resourceList);
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public void addDefaultResforExec(ExecutorDetails exec) {
        Map<String, Map<String, Double>> defaultResourceList = new HashMap<String, Map<String, Double>>();
        defaultResourceList.put((String) topologyConf.get(Config.TOPOLOGY_TYPE_CPU), new HashMap<String, Double>());
        defaultResourceList.get(topologyConf.get(Config.TOPOLOGY_TYPE_CPU)).put((String)topologyConf.get(Config.TOPOLOGY_TYPE_CPU_TOTAL), (Double)topologyConf.get(Config.TOPOLOGY_DEFAULT_CPU_REQUIREMENT));

        defaultResourceList.put((String)topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY), new HashMap<String, Double>());
        defaultResourceList.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY)).put(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB, (Double) topologyConf.get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB));
        defaultResourceList.get(topologyConf.get(Config.TOPOLOGY_TYPE_MEMORY)).put(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB, (Double) topologyConf.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB));
        LOG.warn("Scheduling Executor: {} with memory requirement as onHeap: {} - offHeap: {} " +
                        "and CPU requirement: {}",
                exec, topologyConf.get(Config.TOPOLOGY_RESOURCES_ONHEAP_MEMORY_MB),
                topologyConf.get(Config.TOPOLOGY_RESOURCES_OFFHEAP_MEMORY_MB),
                topologyConf.get(Config.TOPOLOGY_DEFAULT_CPU_REQUIREMENT));
        addResourcesForExec(exec, defaultResourceList);
    }
}
