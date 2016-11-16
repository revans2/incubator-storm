package com.yahoo.storm.metrics;

/**
 * Definitions for Simon in Storm configurations.
 */
public class SimonConfig {
    private SimonConfig() {}

    /**
     * A YAML list of simon servers specified as "$server:$port".
     * Example: "[mysimonhost:8080, myothersimonhost:8080]"
     */
    public static final String SIMON_SERVERS = "simon.servers";
    public static final String SIMON_CLUSTER = "simon.cluster";

    /**
     * The XML file resource containing the Simon config. This should be paired
     * with a corresponding {@link #SIMON_CONFIG_SCHEMA}
     */
    public static final String SIMON_CONFIG = "simon.config";

    /**
     * The XSD file resource containing the schema corresponding to
     * {@link #SIMON_CONFIG}.
     */
    public static final String SIMON_CONFIG_SCHEMA = "simon.config.schema";

    /**
     *
     * The frequency at which config messages are sent to the aggregators. This
     * is an average number of blurb periods between config messages.
     */
    public static final String SIMON_CONFIG_MESSAGE_FREQUENCY = "simon.config.message.frequency";
}
