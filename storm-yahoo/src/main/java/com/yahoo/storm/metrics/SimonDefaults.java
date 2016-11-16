package com.yahoo.storm.metrics;

/**
 * Definitions for Simon in Storm configurations.
 */
public class SimonDefaults {
    private SimonDefaults() {}

    /**
     * The XML file resource containing the Simon config. This should be paired
     * with a corresponding {@link #SIMON_CONFIG_SCHEMA}
     */
    public static final String SIMON_CONFIG = "/conf/simon.xml";

    /**
     * The XSD file resource containing the schema corresponding to
     * {@link #SIMON_CONFIG}.
     */
    public static final String SIMON_CONFIG_SCHEMA = "/conf/simon.xsd";

}
