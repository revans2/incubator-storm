package org.apache.storm.metrics2.store;
import java.util.Set;

public interface IAggregator {
    public void agg(Metric metric, Set<TimeRange> timeRanges);
}
