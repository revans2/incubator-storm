package backtype.storm.scheduler.resource.strategies;

import java.util.Collection;
import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.multitenant.MultitenantScheduler;
import backtype.storm.scheduler.resource.RAS_Node;

public class MultitenantStrategy implements IStrategy{

    /**
     * DO NOT USE! THIS CLASS IS A DUMMY CLASS TO EASE THE MIGRATION TO
     * RESOURCE AWARE SCHEDULING
     */
    @Override
    public Map<RAS_Node, Collection<ExecutorDetails>> schedule(TopologyDetails td) {
        throw new IllegalStateException();
    }


}
