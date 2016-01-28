package backtype.storm.scheduler.resource.strategies;

import java.util.Map;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.ClusterStateData;
import backtype.storm.scheduler.resource.RAS_Nodes;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.User;
import backtype.storm.scheduler.resource.strategies.scheduling.IStrategy;

public class MultitenantStrategy implements IStrategy{

    /**
     * DO NOT USE! THIS CLASS IS A DUMMY CLASS TO EASE THE MIGRATION TO
     * RESOURCE AWARE SCHEDULING
     */
    @Override
    public void prepare(ClusterStateData clusterStateData) {
        throw new IllegalStateException();
    }

    public void prepare(Topologies topologies, Cluster cluster, Map<String, User> userMap, RAS_Nodes nodes) {
        throw new IllegalStateException();
    }

    /**
     * DO NOT USE! THIS CLASS IS A DUMMY CLASS TO EASE THE MIGRATION TO
     * RESOURCE AWARE SCHEDULING
     */
    @Override
    public SchedulingResult schedule(TopologyDetails td) {
        throw new IllegalStateException();
    }
}
