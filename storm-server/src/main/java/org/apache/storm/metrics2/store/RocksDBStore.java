/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;

import org.apache.commons.codec.binary.Hex;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RocksDBStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(RocksDBStore.class);

    //rocksjni instance
    private RocksDB _db;

    // options on how to flush db to disk
    private FlushOptions _fops;

    // metadata instance
    private RocksDBSerializer _serializer;

    private byte _topoMetadataKey = (byte) 0;
    private byte _streamMetadataKey = (byte) 1;
    private byte _hostMetadataKey = (byte) 2;

    /**
     * Implements the prepare method of the Metric Store, create RocksDB instance
     * using the configurations provided via the config map
     *
     * @param config Storm config map
     */
    @Override
    public RocksDBStore prepare(Map config) {
        try {
            validateConfig(config);
        } catch (MetricException e) {
            LOG.error("Invalid config for RocksDB metrics store", e);
            //TODO-AB: throw a runtime error
        }

        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        //Utils.getString
        boolean createIfMissing = Boolean.parseBoolean(config.get("storm.metrics2.store.rocksdb.create_if_missing").toString());
        Options options         = new Options().setCreateIfMissing(createIfMissing);

        if (config.containsKey("storm.metrics2.store.rocksdb.total_threads")) {
            options.setIncreaseParallelism((int) config.get("storm.metrics2.store.rocksdb.total_threads"));
        }

        if (config.containsKey("storm.metrics2.store.rocksdb.optimize_filters_for_hits")) {
            options.setOptimizeFiltersForHits((boolean) config.get("storm.metrics2.store.rocksdb.optimize_filters_for_hits"));
        }

        if (config.containsKey("storm.metrics2.store.rocksdb.optimize_level_style_compaction")) {
            if ((boolean) config.get("storm.metrics2.store.rocksdb.optimize_level_style_compaction")) {
                if (config.containsKey("storm.metrics2.store.rocksdb.optimize_level_style_compaction_memtable_memory_budget_mb")) {
                    Integer budget_mb = (Integer) config.get("storm.metrics2.store.rocksdb.optimize_level_style_compaction_memtable_memory_budget_mb");
                    options.optimizeLevelStyleCompaction(budget_mb * 1024L * 1024L);
                } else {
                    options.optimizeLevelStyleCompaction();
                }
            }
        }

        LOG.info("Instantiating RocksDB BlockBasedTable");

        // use the hash index for prefix searches
        BlockBasedTableConfig tfc = new BlockBasedTableConfig();
        tfc.setIndexType(IndexType.kHashSearch);
        options.setTableFormatConfig(tfc);
        options.useCappedPrefixExtractor(48); // current key is 48 bytes long

        _db = null;
        try {
            // a factory method that returns a RocksDB instance
            String path = config.get("storm.metrics2.store.rocksdb.location").toString();
            _db = RocksDB.open(options, path);
            // do something
        } catch (RocksDBException e) {
            LOG.error("Error opening RockDB database", e);
        }

        LOG.info("RocksDB Stats: {}", getStats());

        _serializer = new RocksDBSerializer();

        // restore metadata
        LOG.info("Restoring metadata");
        scanKV(_serializer.makeKey(_topoMetadataKey, null), (key, value) -> {
            return _serializer.putToTopoMap(key, value);
        });
        scanKV(_serializer.makeKey(_streamMetadataKey, null), (key, value) -> {
            return _serializer.putToStreamMap(key, value);
        });
        scanKV(_serializer.makeKey(_hostMetadataKey, null), (key, value) -> {
            return _serializer.putToHostMap(key, value);
        });

        LOG.info("Metadata topos {}", _serializer.contents());

        _fops = new FlushOptions();
        _fops.setWaitForFlush(true);

        return this;
    }

    public String getStats() {
        String stats = null;
        try {
            stats = _db.getProperty("rocksdb.stats");
        } catch (RocksDBException e) {
            LOG.error("Error getting RockDB database stats", e);
        }
        return stats;
    }

    private RocksDB getDb(Metric m) {
        // given a metric and sharding config, return the rocksdb instance
        // note: it might be a single instance, or it might be one per owner
        return _db;
    }

    private RocksDB getMetadataDb() {
        // this could become a different database later
        return _db;
    }

    /**
     * Implements the insert method of the Metric Store, stores metrics in the store
     *
     * @param m Metric to store
     */
    @Override
    public void insert(Metric m) {
        try {
            LOG.info("inserting {}", m.toString());

            RocksDB db     = this.getDb(m);
            RocksDB metaDb = this.getMetadataDb();

            // load metadata
            if (!_serializer.metaInitialized(m)) {
                _serializer.deserializeMeta(m.getTopoIdStr(), db.get(_serializer.metadataKey(m)));
            }

            // add mapping in memory
            Integer topoId   = _serializer.getTopoId(m.getTopoIdStr());
            Integer hostId   = _serializer.getHostId(m.getHost());
            Integer streamId = _serializer.getStreamId(m.getStream());

            // persist metadata in rocksdb
            try {
                metaDb.put(_serializer.makeKey(_topoMetadataKey, topoId), m.getTopoIdStr().getBytes("UTF-8"));
                metaDb.put(_serializer.makeKey(_hostMetadataKey, hostId), m.getHost().getBytes("UTF-8"));
                metaDb.put(_serializer.makeKey(_streamMetadataKey, streamId), m.getStream().getBytes("UTF-8"));
            } catch (java.io.UnsupportedEncodingException ex) {
                LOG.error("Unsupoorted encoding!", ex);
                return;
            }

            RocksDBSerializer.SerializationResult sr = _serializer.serialize(m);
            db.put(sr.metricKey, sr.metricValue);

            // TODO: perhaps this becomes flat against rocksdb too
            if (sr.metaTopoValue != null) {
                db.put(_serializer.metadataKey(m), sr.metaTopoValue);
            }

            db.flush(_fops);
        } catch (RocksDBException e) {
            LOG.error("Error inserting into RocksDB", e);
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics in the store
     *
     * @param agg Callback fn, called as we are scanning through store
     * @return void
     */

    @Override
    public void scan(IAggregator agg) {
        long test = 0L;
        // TODO: for each topo stored in serializer
        // iterate over it using its own db
        for (String topoId : _serializer.getTopoIds()) {
            LOG.debug("full scanning for topology {}", topoId);
            HashMap<String, Object> settings = new HashMap<String, Object>();
            settings.put(StringKeywords.topoId, topoId);
            scan(settings, agg);
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics with settings in the store
     * Will try to search the fastest way possible
     *
     * @param settings map of settings to search by
     * @param agg      Callback fn, called as we are scanning through store
     * @return void
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {
        // load metadata
        // TODO: make function that can take the settings and decide
        // whether we have enough info to find the db
        // rather than construct a metric
        Metric m         = new RocksDBMetric();
        String topoIdStr = (String) settings.get(StringKeywords.topoId);
        m.setTopoIdStr(topoIdStr);

        // RocksDB end time stamps are inclusive, we want exclusive
        HashSet<TimeRange> timeSet = (HashSet<TimeRange>) settings.get(StringKeywords.timeRangeSet);
        if (timeSet != null) {
            timeSet.forEach(tr -> {
                if (tr.endTime != null) tr.endTime -= 1;
            });
        }

        try {
            if (!_serializer.metaInitialized(m)) {
                _serializer.deserializeMeta(m.getTopoIdStr(), getDb(m).get(_serializer.metadataKey(m)));
            }
        } catch (RocksDBException ex) {
            LOG.error("Error loading metadata", ex);
        }

        byte[] prefix = _serializer.createPrefix(settings);
        if (prefix != null) {
            scan(prefix, settings, agg);
            return;
        }
        LOG.error("Couldn't obtain prefix");
    }

    @Override
    public void remove(HashMap<String, Object> settings) {
        // for each key we match, remove it
        scan(settings, (metric, timeRanges) -> {
            remove(metric);
        });
    }

    public void remove(Metric keyToRemove) {
        try {
            getDb(keyToRemove).remove(((RocksDBMetric) keyToRemove).getKey());
        } catch (RocksDBException ex) {
            LOG.error("Exception while removing {}", keyToRemove);
        }
    }

    @Override
    public boolean populateValue(Metric m) {
        RocksDB db = this.getDb(m);

        try {
            // load metadata
            if (!_serializer.metaInitialized(m)) {
                _serializer.deserializeMeta(m.getTopoIdStr(), db.get(_serializer.metadataKey(m)));
            }

            RocksDBSerializer.SerializationResult sr = _serializer.serialize(m);

            byte[] value = _db.get(sr.metricKey);
            _serializer.populate(m, value);
            return value != null ? true : false;
        } catch (RocksDBException ex) {
            LOG.error("Exception getting value:", ex);
            return false;
        }
    }

    /**
     * Implements scan method of the Metrics Store, scans all metrics with prefix in the store
     *
     * @param prefix prefix to query in store
     */
    private void scanKV(byte[] prefix, IScanCallback fn) {
        String prefixStr = Hex.encodeHexString(prefix);
        LOG.info("Prefix kv scan with {} and length {}", prefixStr, prefix.length);
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(true);
        RocksIterator iterator = _db.newIterator(ro);
        iterator.seekToFirst();
        long startTime         = System.nanoTime();
        long numRecords        = 0;
        long numScannedRecords = 0;
        for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
            //String key = new String(iterator.key());

            // put metadata to _serializer maps
            if (!fn.cb(iterator.key(), iterator.value())) {
                // if cb returns false, we are done with this section of
                // rows
                return;
            }
            numRecords++;
            numScannedRecords++;
        }
        LOG.info("prefix scan complete for {} with {} records and {} scanned total in {} ms",
                prefixStr, numRecords, numScannedRecords, (System.nanoTime() - startTime) / 1000000);
    }

    private void scan(byte[] prefix, HashMap<String, Object> settings, IAggregator agg) {
        String prefixStr = Hex.encodeHexString(prefix);
        LOG.info("Prefix scan with {} and length {}", prefixStr, prefix.length);
        ReadOptions ro = new ReadOptions();
        ro.setTotalOrderSeek(true);

        //TODO: somehow, either get the reference for the db or just use a single db for everything
        RocksIterator iterator = _db.newIterator(ro);
        iterator.seekToFirst();
        long startTime         = System.nanoTime();
        long numRecords        = 0;
        long numScannedRecords = 0;
        for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
            //String key = new String(iterator.key());

            Metric metric = _serializer.deserialize(iterator.key());
            if (metric == null) {
                // if we can't deserialize (e.g. key type is metadata)
                // we should be done
                break;
            }
            LOG.debug("Scanning key: {}", metric.toString());

            Set<TimeRange> timeRanges = checkRequiredSettings(metric, settings);
            numScannedRecords++;
            if (timeRanges != null) {
                boolean include = checkMetric(metric, settings);
                if (include) {
                    LOG.debug("Match key: {}", Hex.encodeHexString(iterator.key()));
                    numRecords++;
                    if (!_serializer.metaInitialized(metric)) {
                        _serializer.deserializeMeta(metric.getTopoIdStr(), _serializer.metadataKey(metric));
                    }
                    _serializer.populate(metric, iterator.value());
                    agg.agg(metric, timeRanges);
                }
            } else {
                // if we don't find in required settings, no need to continue
                break;
            }
        }
        LOG.info("prefix scan complete for {} with {} records and {} scanned total in {} ms",
                prefixStr, numRecords, numScannedRecords, (System.nanoTime() - startTime) / 1000000);
    }

    /**
     * Implements configuration validation of Metrics Store, validates storm configuration for Metrics Store
     *
     * @param config Storm config to specify which store type, location of store and creation policy
     * @throws MetricException if there is a missing required configuration or if the store does not exist but
     *                         the config specifies not to create the store
     */
    private void validateConfig(Map config) throws MetricException {
        if (!(config.containsKey("storm.metrics2.store.rocksdb.location"))) {
            throw new MetricException("Not a vaild RocksDB configuration - Missing store location");
        }

        if (!(config.containsKey("storm.metrics2.store.rocksdb.create_if_missing"))) {
            throw new MetricException("Not a vaild RocksDB configuration - Does not specify creation policy");
        }

        String createIfMissing = config.get("storm.metrics2.store.rocksdb.create_if_missing").toString();
        if (!Boolean.parseBoolean(createIfMissing)) {
            String storePath = config.get("storm.metrics2.store.rocksdb.location").toString();
            if (!(new File(storePath).exists())) {
                throw new MetricException("Configuration specifies not to create a store but no store currently exists");
            }
        }
        return;
    }

    /**
     * Implements configuration validation of Metrics Store, validates storm configuration for Metrics Store
     *
     * @param possibleKey key to check
     * @param settings    search settings
     * @throws MetricException if there is a missing required configuration or if the store does not exist but
     *                         the config specifies not to create the store
     */
    private boolean checkMetric(Metric possibleKey, HashMap<String, Object> settings) {
        LOG.info("Checking {}", possibleKey);
        if (settings.containsKey(StringKeywords.component) &&
                !possibleKey.getCompName().equals(settings.get(StringKeywords.component))) {
            LOG.info("Not the right component {}", possibleKey.getCompName());
            return false;
        } else if (settings.containsKey(StringKeywords.metricSet) &&
                !((HashSet<String>) settings.get(StringKeywords.metricSet)).contains(possibleKey.getMetricName())) {
            LOG.info("Not the right metric name {}", possibleKey.getMetricName());
            return false;
        }
        return true;
    }

    private Set<TimeRange> checkRequiredSettings(Metric possibleKey, HashMap<String, Object> settings) {
        LOG.debug("Checking metric {} key {}", possibleKey, Hex.encodeHexString(((RocksDBMetric) possibleKey).getKey()));
        Integer aggLevelInt = (Integer) settings.get(StringKeywords.aggLevel);
        Byte    aggLevel    = (aggLevelInt != null) ? aggLevelInt.byteValue() : (byte) 0;

        LOG.info("compare agg level: {} {}", possibleKey.getAggLevel(), aggLevel);

        if (settings.containsKey(StringKeywords.aggLevel) &&
                (possibleKey.getAggLevel() == null ||
                        !possibleKey.getAggLevel().equals(aggLevel))) {
            LOG.info("DOES NOT MATCH agg level: {} {}", possibleKey.getAggLevel(), ((Integer) settings.get(StringKeywords.aggLevel)).byteValue());
            return null;
        } else if (settings.containsKey(StringKeywords.topoId) &&
                !possibleKey.getTopoIdStr().equals(settings.get(StringKeywords.topoId))) {
            LOG.info("DOES NOT MATCH topo {} {}", possibleKey.getTopoIdStr(), settings.get(StringKeywords.topoId));
            return null;
        } else if (settings.containsKey(StringKeywords.timeRangeSet)) {
            Set<TimeRange> timeRangeSet        = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);
            Set<TimeRange> initialTimeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.initialTimeRangeSet);
            Set<TimeRange> matchedTimeRanges   = new HashSet<TimeRange>();
            Long           tstamp              = possibleKey.getTimeStamp();

            for (TimeRange tr : timeRangeSet) {
                if (tr.contains(tstamp)) {
                    for (TimeRange initialTimeRange : initialTimeRangeSet) {
                        if (initialTimeRange.contains(tr))
                            matchedTimeRanges.add(initialTimeRange);
                    }
                }
            }

            return matchedTimeRanges.size() > 0 ? matchedTimeRanges : null;
        }

        return null;
    }

}
