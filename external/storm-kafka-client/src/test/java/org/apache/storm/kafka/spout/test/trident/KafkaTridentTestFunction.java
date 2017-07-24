package org.apache.storm.kafka.spout.test.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Test Trident function which prints all input tuples
 */
public class KafkaTridentTestFunction extends BaseFunction {
    protected static final Logger LOG = LoggerFactory.getLogger(KafkaTridentTestFunction.class);
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        LOG.debug("input = [" + tuple + "]");
        collector.emit(tuple);
    }
}
