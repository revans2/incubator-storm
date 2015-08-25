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
import backtype.storm.scheduler.resource.strategies.IStrategy;
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
    private Map<ExecutorDetails, Map<String, Double>> _resourceList;
    //Scheduler this topology should be scheduled with
    private String scheduler;

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
        this.initScheduler();
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
        _resourceList = new HashMap<ExecutorDetails, Map<String, Double>>();
        // Extract bolt memory info
        if (this.topology.get_bolts() != null) {
            for (Map.Entry<String, Bolt> bolt : this.topology.get_bolts().entrySet()) {
                //the json_conf is populated by TopologyBuilder (e.g. boltDeclarer.setMemoryLoad)
                Map<String, Double> topology_resources = this.parseResources(bolt
                        .getValue().get_common().get_json_conf());
                this.checkIntialization(topology_resources, bolt.getValue().toString());
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent : executorToComponent.entrySet()) {
                    if (bolt.getKey().equals(anExecutorToComponent.getValue())) {
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
                Map<String, Double> topology_resources = this.parseResources(spout
                        .getValue().get_common().get_json_conf());
                this.checkIntialization(topology_resources, spout.getValue().toString());
                for (Map.Entry<ExecutorDetails, String> anExecutorToComponent : executorToComponent.entrySet()) {
                    if (spout.getKey().equals(anExecutorToComponent.getValue())) {
                        _resourceList.put(anExecutorToComponent.getKey(), topology_resources);
                    }
                }
            }
        } else {
            LOG.warn("Topology " + topologyId + " does not seem to have any spouts!");
        }
        //schedule tasks that are not part of components returned from topology.get_spout or topology.getbolt (AKA sys tasks most specifically __acker tasks)
        for(ExecutorDetails exec : this.getExecutors()) {
            if (_resourceList.containsKey(exec) == false) {
                LOG.info(
                        "Scheduling {} {} with memory requirement as 'on heap' - {} and 'off heap' - {} and CPU requirement as {}",
                        this.getExecutorToComponent().get(exec),
                        exec,
                        this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB),
                        this.topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB),
                        this.topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
                this.addDefaultResforExec(exec);
            } 
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
    private Map<String, Double> parseResources(String input) {
        Map<String, Double> topology_resources = new HashMap<String, Double>();
        JSONParser parser = new JSONParser();
        LOG.debug("Input to parseResources {}", input);
        try {
            if (input != null) {
                Object obj = parser.parse(input);
                JSONObject jsonObject = (JSONObject) obj;
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
                    Double topoMemOnHeap = Utils.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, topoMemOnHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
                    Double topoMemOffHeap = Utils.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, topoMemOffHeap);
                }
                if (jsonObject.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
                    Double topoCPU = Utils.getDouble(jsonObject.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null);
                    topology_resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, topoCPU);
                }
                LOG.debug("Topology Resources {}", topology_resources);
            }
        } catch (ParseException e) {
            LOG.error("Failed to parse component resources is:" + e.toString(), e);
            return null;
        }
        return topology_resources;
    }

    private void checkIntialization(Map<String, Double> topology_resources, String Com) {
        this.checkInitMem(topology_resources, Com);
        this.checkInitCPU(topology_resources, Com);
    }

    private void debugMessage(String memoryType, String Com)
    {
        if (memoryType.equals("ONHEAP"))
        {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : Memory Type : On Heap set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB));
        }
        else if (memoryType.equals("OFFHEAP"))
        {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : Memory Type : Off Heap set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB));
        }
        else
        {
            LOG.debug(
                    "Unable to extract resource requirement for Component {} \n Resource : CPU Pcore Percent set to default {}",
                    Com, topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
        }
    }

    private void checkInitMem(Map<String, Double> topology_resources, String Com) {
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                    Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null));
            debugMessage("ONHEAP", Com);
        }
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                    Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null));
            debugMessage("OFFHEAP", Com);
        }
    }

    private void checkInitCPU(Map<String, Double> topology_resources, String Com) {
        if (!topology_resources.containsKey(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT)) {
            topology_resources.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
                            Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null));
            debugMessage("CPU", Com);
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
        if (storm_topo.get_spouts() != null) {
            for (Map.Entry<String, SpoutSpec> spoutEntry : storm_topo
                    .get_spouts().entrySet()) {
                if (!Utils.isSystemId(spoutEntry.getKey())) {
                    RAS_Component newComp = null;
                    if (all_comp.containsKey(spoutEntry.getKey())) {
                        newComp = all_comp.get(spoutEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                    } else {
                        newComp = new RAS_Component(spoutEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                        all_comp.put(spoutEntry.getKey(), newComp);
                    }
                    newComp.type = RAS_Component.ComponentType.SPOUT;

                    for (Map.Entry<GlobalStreamId, Grouping> spoutInput : spoutEntry
                            .getValue().get_common().get_inputs()
                            .entrySet()) {
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
        if (storm_topo.get_bolts() != null) {
            for (Map.Entry<String, Bolt> boltEntry : storm_topo.get_bolts()
                    .entrySet()) {
                if (!Utils.isSystemId(boltEntry.getKey())) {
                    RAS_Component newComp = null;
                    if (all_comp.containsKey(boltEntry.getKey())) {
                        newComp = all_comp.get(boltEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                    } else {
                        newComp = new RAS_Component(boltEntry.getKey());
                        newComp.execs = componentToExecs(newComp.id);
                        all_comp.put(boltEntry.getKey(), newComp);
                    }
                    newComp.type = RAS_Component.ComponentType.BOLT;

                    for (Map.Entry<GlobalStreamId, Grouping> boltInput : boltEntry
                            .getValue().get_common().get_inputs()
                            .entrySet()) {
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
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB);
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
            ret = _resourceList
                    .get(exec)
                    .get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB);
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
            return _resourceList
                      .get(exec)
                      .get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT);
        }
        LOG.info("cannot find - {}", exec);
        return null;
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public Map<String, Double> getTaskResourceReqList(ExecutorDetails exec) {
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
        if (_resourceList != null) { // null is possible if the first constructor of TopologyDetails is used
            return _resourceList.containsKey(exec);
        } else {
            return false;
        }
    }

    /**
     * Note: The public API relevant to resource aware scheduling is unstable as of May 2015.
     *       We reserve the right to change them.
     */
    public void addResourcesForExec(ExecutorDetails exec, Map<String, Double> resourceList) {
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
        Map<String, Double> defaultResourceList = new HashMap<String, Double>();
        defaultResourceList.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT,
                        Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT), null));
        defaultResourceList.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB,
                        Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB), null));
        defaultResourceList.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB,
                        Utils.getDouble(topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB), null));
        LOG.warn("Scheduling Executor: {} with memory requirement as onHeap: {} - offHeap: {} " +
                        "and CPU requirement: {}",
                exec, topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB),
                topologyConf.get(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB),
                topologyConf.get(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT));
        addResourcesForExec(exec, defaultResourceList);
    }

    /**
     * initalizes the scheduler member variable by extracting what scheduler
     * this topology is going to use from topologyConf
     */
    private void initScheduler() {
        this.scheduler = Utils.getString(this.topologyConf.get(Config.TOPOLOGY_SCHEDULER_STRATEGY), null);
    }

    public String getScheduler() {
        return this.scheduler;
    }
    
    public void setTopologyStrategy(Class<? extends IStrategy> clazz) {
        if(clazz != null) {
            this.scheduler = clazz.getName();
        }
        
    }
}
