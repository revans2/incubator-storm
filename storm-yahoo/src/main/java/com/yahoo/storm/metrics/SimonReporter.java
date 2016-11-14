package com.yahoo.storm.metrics;

import org.apache.storm.shade.com.codahale.metrics.MetricFilter;
import org.apache.storm.shade.com.codahale.metrics.MetricRegistry;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimonReporter implements PreparableReporter {
    private final static Logger LOG = LoggerFactory.getLogger(SimonReporter.class);
    private volatile boolean _isMonitoring = false;
    private SimonScheduledReporter _reporter;


    private List<InetSocketAddress> parseServersConfig(Map<String, ?> conf) {
        List<String> _metricServerNames =
                (List<String>) conf.get(SimonConfig.SIMON_SERVERS);
        List<InetSocketAddress> ret =
                new ArrayList<>(_metricServerNames.size());
        for (String hp : _metricServerNames) {
            String[] host_port = hp.split(":", 2);
            int port = host_port.length < 2 || host_port[1].isEmpty() ?
                    SimonScheduledReporter.DEFAULT_PORT :
                    Integer.valueOf(host_port[1]);
            LOG.info("Configuring connection to aggregator " + host_port[0] + ":" + port);
            ret.add(new InetSocketAddress(host_port[0], port));
        }
        return ret;
    }

    public boolean isMonitoring() {
        return _isMonitoring;
    }

    @Override
    public void prepare(MetricRegistry metricsRegistry, Map stormConf) {
        List<InetSocketAddress> metricServerSocketAddresses =
                parseServersConfig(stormConf);
        String cluster = (String) stormConf.get(SimonConfig.SIMON_CLUSTER);
        String simonConf =
                (String) stormConf.get(SimonConfig.SIMON_CONFIG);
        String simonSchema =
                (String) stormConf.get(SimonConfig.SIMON_CONFIG_SCHEMA);
        LOG.info("Starting Simon for cluster " + cluster + " with confg " + simonConf + " and schema " + simonSchema);
        int simonConfMsgFreq = (int) stormConf.getOrDefault(
                SimonConfig.SIMON_CONFIG_MESSAGE_FREQUENCY,
                SimonScheduledReporter.DEFAULT_CONFIG_MESSAGE_FREQ);
        _reporter = new SimonScheduledReporter(
                metricsRegistry,
                MetricFilter.ALL,
                TimeUnit.SECONDS,
                TimeUnit.MILLISECONDS,
                metricServerSocketAddresses,
                cluster,
                simonConf,
                simonSchema,
                simonConfMsgFreq);
    }

    @Override
    public void start() {
        if ( _reporter != null ) {
            LOG.debug("Starting...");
            _reporter.start(5, TimeUnit.SECONDS);
        } else {
            throw new IllegalStateException("Attempt to start without preparing " + getClass().getSimpleName());
        }
    }

    @Override
    public void stop() {
        if ( _reporter != null )  {
            LOG.debug("Stopping...");
            _reporter.stop();
        } else {
            throw new IllegalStateException("Attempt to stop without preparing " + getClass().getSimpleName());
        }
    }
}
