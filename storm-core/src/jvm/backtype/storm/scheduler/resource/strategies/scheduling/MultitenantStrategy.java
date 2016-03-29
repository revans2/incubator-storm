package backtype.storm.scheduler.resource.strategies.scheduling;

import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.resource.SchedulingResult;
import backtype.storm.scheduler.resource.SchedulingState;

public class MultitenantStrategy implements IStrategy{

    /**
     * DO NOT USE! THIS CLASS IS A DUMMY CLASS TO EASE THE MIGRATION TO
     * RESOURCE AWARE SCHEDULING
     */
    @Override
    public void prepare(SchedulingState schedulingState) {

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
