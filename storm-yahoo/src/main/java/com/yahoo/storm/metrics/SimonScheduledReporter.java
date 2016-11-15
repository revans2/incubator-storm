package com.yahoo.storm.metrics;

import org.apache.storm.shade.com.codahale.metrics.Counter;
import org.apache.storm.shade.com.codahale.metrics.Gauge;
import org.apache.storm.shade.com.codahale.metrics.Histogram;
import org.apache.storm.shade.com.codahale.metrics.Meter;
import org.apache.storm.shade.com.codahale.metrics.MetricFilter;
import org.apache.storm.shade.com.codahale.metrics.MetricRegistry;
import org.apache.storm.shade.com.codahale.metrics.ScheduledReporter;
import org.apache.storm.shade.com.codahale.metrics.Timer;
import com.yahoo.simon.config.*;
import com.yahoo.simon.config.SimonConfig;
import com.yahoo.simon.hadoop.metrics.BlurbType;
import com.yahoo.simon.hadoop.metrics.Client;
import com.yahoo.simon.hadoop.metrics.SimonContext;
import com.yahoo.simon.hadoop.metrics.Signature;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimonScheduledReporter extends ScheduledReporter {
    private final static Logger LOG = LoggerFactory.getLogger(SimonScheduledReporter.class);
    private List<InetSocketAddress> _servers;
    private SimonConfig _config;
    private byte[] _compressedConfig;
    private Client _client;
    private String _ip;
    private String _hostname;
    private Map<String,BlurbType> blurbTypeMap = new HashMap<String,BlurbType>();
    private Map<BlurbType,HashSet> metricMap = new HashMap<BlurbType,HashSet>();
    private Boolean sentFullBlurb = false;

    // Default port is "SIMO" on a telephone key pad
    static final int DEFAULT_PORT = 7466;
    static final int DEFAULT_CONFIG_MESSAGE_FREQ = 100;

    public SimonScheduledReporter(MetricRegistry registry,
                                  MetricFilter filter,
                                  TimeUnit rateUnit,
                                  TimeUnit durationUnit,
                                  List<InetSocketAddress> servers,
                                  String cluster,
                                  String conf,
                                  String schema,
                                  int confMsgFreq) {
        super(registry, "simon-reporter", filter, rateUnit, durationUnit);
        initConfigs(servers, cluster, conf, schema, confMsgFreq);
        initBlurbTypeMap();
        initHostTags();
    }

    private void initHostTags() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface iface = interfaces.nextElement();
                if (iface.isLoopback() || !iface.isUp())
                    continue;

                Enumeration<InetAddress> addresses = iface.getInetAddresses();
                while(addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    _ip = addr.getHostAddress();
                    _hostname = InetAddress.getByName(_ip).getHostName();
                }
            }
        } catch (UnknownHostException | SocketException e) {
            throw new RuntimeException(e);
        }
    }

    private void initBlurbTypeMap() {
        byte blurbCount = 0;
        for (BlurbConfig blurbConfig : _config.getBlurbMessage().getBlurbs()) {
            String name = blurbConfig.getName();
            BlurbType blurbType = new BlurbType(blurbConfig, ++blurbCount);
            LOG.info("Adding blurb type " + blurbType + " for " + name);
            blurbTypeMap.put(name, blurbType);
            List<MetricConfig> metrics = blurbConfig.getMetrics();
            HashSet<String> nameSet = new HashSet<String>();
            for (MetricConfig metric : blurbConfig.getMetrics()) {
                nameSet.add(metric.getName());
            }
            metricMap.put(blurbType, nameSet);
        }
    }

    private void initConfigs(List<InetSocketAddress> servers,
                             String cluster,
                             String conf,
                             String schema,
                             int confMsgFreq) {
        initServersConfig(servers);
        initSimonConfigs(conf, schema);
        try {
            Signature sig = new Signature(_config.getBlurbMessage().getName(),
                                          _config.getBlurbMessage().getVersion().toInt(),
                                          _config.getBlurbMessage().computeHash());
            _client = new Client(
                    _servers,
                    cluster,
                    _config.getAppName(),
                    sig,
                    new SimonContext().getReportSigs(_config),
                    _compressedConfig,
                    System.currentTimeMillis(),
                    confMsgFreq);
        } catch (SocketException | UnknownHostException e) {
            throw new RuntimeException("Could not create Simon client.", e);
        }
    }

    private void initSimonConfigs(String conf, String schema) {
        LOG.info("About to init config " + conf + " with schema " + schema);
        
        InputStream confStream = null;
        InputStream schemaStream = null;
        try {
            confStream = new FileInputStream(conf);
        } catch (FileNotFoundException e) { // Ignore exception.  We'll try loading conf from jar.
            confStream = getClass().getResourceAsStream(SimonDefaults.SIMON_CONFIG);
        }
        try {
            schemaStream = new FileInputStream(schema);
        } catch (FileNotFoundException e) { // Ignore exception.  We'll try loading schema from jar.
            schemaStream = getClass().getResourceAsStream(SimonDefaults.SIMON_CONFIG_SCHEMA);
        }
        if (confStream == null) {
            throw new IllegalArgumentException( "Failed to open simon config " + conf + " or from resource " + SimonDefaults.SIMON_CONFIG);
        }
        if (schemaStream == null) {
            throw new IllegalArgumentException( "Failed to open simon config schema " + schema + " or from resource " + SimonDefaults.SIMON_CONFIG_SCHEMA);
        }
        _config = new SimonConfig();
        try {
            _config.parseStream(confStream, schemaStream);
            try {
                confStream = new FileInputStream(conf);
            } catch (FileNotFoundException e) { // Ignore exception.  We'll try loading conf from jar.
                confStream = getClass().getResourceAsStream(SimonDefaults.SIMON_CONFIG);
            }
            _compressedConfig = _config.getCompressedConfig(confStream);
        } catch (IOException | ConfigException e) {
            throw new IllegalArgumentException(
                    "Failed to parse Simon config", e);
        }
    }

    private void initServersConfig(List<InetSocketAddress> servers) {
        _servers = (servers == null || servers.isEmpty()) ?
                Collections.singletonList(
                        new InetSocketAddress("localhost",
                                DEFAULT_PORT)) :
                servers;
        for (InetSocketAddress s : _servers) {
            LOG.debug("Configured to send Simon metrics to " + s);
        }
    }

    private BlurbType getBlurbTypeForMetric( String metricName ) {
        String convertedMetricName = metricName.replace('-','_');
        String[] splitIt = convertedMetricName.split("[\\.:]");
        int pos = splitIt.length - 2;
        if ( pos >= 0) {
            return blurbTypeMap.get(splitIt[pos]);
        }

        LOG.warn("No blurb type for metric " + metricName);
        return null;
    }

    private String getFieldNameForMetric( String metricName ) {
        String convertedMetricName = metricName.replace('-','_');
        String[] splitIt = convertedMetricName.split("[\\.:]");
        int pos = splitIt.length - 1;
        if ( pos >= 0) {
            return splitIt[pos];
        }

        return null;
    }

    private void checkAndAddMetric( String metricName, Object metricValue, Map<BlurbType, Map<String,Object>> blurbTypeToObjMap ) {

        BlurbType bt = getBlurbTypeForMetric(metricName);
        if (bt == null) {
            LOG.warn("No blurb type for metric " + metricName);
            return;
        }

        HashSet<String> nameSet = metricMap.get(bt);
        if ( nameSet == null ) {
            LOG.warn("No metric names for " + metricName);
            return;
        }

        String fieldName = getFieldNameForMetric(metricName);
        if (!nameSet.contains(fieldName)) {
            LOG.warn("Metric " + fieldName + " does not exist in blurb type " + metricName );
            return;
        }

        if ( blurbTypeToObjMap.get(bt) == null ) {
            blurbTypeToObjMap.put(bt, new HashMap<String, Object>());
        }

        LOG.debug("About to create metric entry for " + fieldName + " with value of " + metricValue.toString());
        blurbTypeToObjMap.get(bt).put(fieldName, metricValue);
    }

    public void report(SortedMap<String, Gauge> guages,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters) {

        Map<BlurbType, Map<String,Object>> blurbTypeToObjMap = new HashMap<>(blurbTypeMap.size());

        // Let's go through each type of metric.
        if (!guages.isEmpty()) {
            for (Map.Entry<String, Gauge> entry : guages.entrySet()) {
                checkAndAddMetric( entry.getKey(), entry.getValue().getValue(), blurbTypeToObjMap );
            }
        }

        if (!counters.isEmpty()) {
            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                checkAndAddMetric( entry.getKey(), (int) entry.getValue().getCount(), blurbTypeToObjMap );
            }
        }

        if (!histograms.isEmpty()) {
            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                checkAndAddMetric( entry.getKey(), (int) entry.getValue().getCount(), blurbTypeToObjMap );
            }
        }

        if (!meters.isEmpty()) {
            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                checkAndAddMetric( entry.getKey(), (int) entry.getValue().getCount(), blurbTypeToObjMap );
            }
        }

        if ( _client == null ) {
            throw new RuntimeException("Simon client not created");
        }

        Boolean sentOne = false;
        for (Map.Entry<BlurbType, Map<String,Object>> entry : blurbTypeToObjMap.entrySet()) {
            BlurbType type = entry.getKey();
            Map<String,Object> objMap = entry.getValue();
            // Our blurbs should be empty for most blurb types.  Skip the empty ones.
            if ( objMap == null || objMap.isEmpty()) {
                LOG.warn("Skipping empty obj map.");
                continue;
            }

            // If there are entries in the blurb type that aren't filled by our map, we'll get an
            // exception when it is sent.  So let's check for that.
            HashSet<String> nameSet = metricMap.get(type);
            Boolean hasAll = true;
            for (String name : nameSet) {
                if ( !objMap.containsKey(name) && !sentFullBlurb ) {
                    if (!sentFullBlurb) {
                        LOG.warn("Obj map missing metric " + name + ". Send will fail.");
                    }
                    hasAll = false;
                }
            }

            if (hasAll) {
                // Add in tags for ip and host.  They are defined in every blurb type
                objMap.put("Hostname", _hostname);
                objMap.put("IpAddress", _ip);
                try {
                    _client.sendBlurb(type,objMap);
                    // Client only sends conf on a flush call, so flush every write.
                    _client.flush();
                    sentOne = true;
                } catch (IOException e) {
                    throw new RuntimeException("Client error sending metrics", e);
                }
            }
        }
        // Remember if we sent any blurb successfully.  This prevents the log from getting
        // filled with endless warnings if something gets misconfigured. 
        sentFullBlurb = sentOne;
    }
}
