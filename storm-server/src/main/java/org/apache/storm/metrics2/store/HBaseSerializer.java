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


import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.storm.metrics2.store.HBaseMetadataIndex.*;

public abstract class HBaseSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseSerializer.class);

    private class MetaData {
        public HashMap<String, Integer> map;
        public HashMap<Integer, String> rmap;
        public HTableInterface table;
    }

    private final static int KEY_LENGTH = 33;

    protected HConnection _hbaseConnection;
    protected HBaseSchema _schema;

    private MetaData[] metaData;

    /**
     * Create appropriate serializer based on schema
     *
     * @param hbaseConnection HConnection instance
     * @param schema          Schema from storm config
     * @return HBaseSerializer instance
     * @throws MetricException On initialization failure
     */
    public static HBaseSerializer createSerializer(HConnection hbaseConnection, HBaseSchema schema)
            throws MetricException {

        HBaseSchemaType type = schema.getSchemaType();
        try {
            Class<?>       clazz = Class.forName(type.getClassName());
            Constructor<?> ctor  = clazz.getConstructor(HConnection.class, HBaseSchema.class);
            return (HBaseSerializer) ctor.newInstance(hbaseConnection, schema);
        } catch (Exception e) {
            throw new MetricException("Could not initialize serializer - " + e);
        }
    }

    /**
     * Serializer constructor
     *
     * @param hbaseConnection HConnection instance
     * @param schema          Schema from storm config
     */
    public HBaseSerializer(HConnection hbaseConnection, HBaseSchema schema) {

        this._hbaseConnection = hbaseConnection;
        this._schema = schema;
        this.metaData = new MetaData[HBaseMetadataIndex.count()];

        for (HBaseMetadataIndex index : HBaseMetadataIndex.values()) {
            assignMetaDataTable(index);
            initializeMap(index);
        }
    }

    /**
     * Initialize metadata information. Create metadata table and refcounter
     * if it doesnt exist.
     *
     * @param meta HBaseMetadataIndex
     */
    private void assignMetaDataTable(HBaseMetadataIndex meta) {
        int i = meta.getIndex();
        try {
            HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];

            this.metaData[i] = new MetaData();
            this.metaData[i].table = _hbaseConnection.getTable(info.getTableName());

            // set column counter
            byte[] refcounter = info.getRefcounter();
            byte[] value      = Bytes.toBytes(0L);
            byte[] cf         = info.getColumnFamily();
            byte[] column     = info.getColumn();
            Put    p          = new Put(refcounter);
            p.add(cf, column, value);

            metaData[i].table.checkAndPut(refcounter, cf, column, null, p);

        } catch (IOException e) {
            LOG.error("Could not assign metadata table", e);
        }
    }


    /**
     * Initialize string-to-integer key mapping caches
     *
     * @param meta Metadata cache to initialize
     */
    private void initializeMap(HBaseMetadataIndex meta) {
        int i = meta.getIndex();
        if (metaData[i].map == null)
            metaData[i].map = new HashMap<>();
        if (metaData[i].rmap == null)
            metaData[i].rmap = new HashMap<>();

        Scan   s            = new Scan();
        byte[] columnFamily = _schema.metadataTableInfos[i].getColumnFamily();
        byte[] column       = _schema.metadataTableInfos[i].getColumn();
        s.addColumn(columnFamily, column);

        try {
            ResultScanner scanner = metaData[i].table.getScanner(s);

            scanner.forEach((result) -> {
                byte[] key   = result.getRow();
                byte[] value = result.getValue(columnFamily, column);

                // NOTE: key REFCOUNTER stores a long
                // valueInteger returns int from upper 4 bytes of stored long
                // Thus, REFCOUNTER value in local map will always be incorrect
                String  keyString    = Bytes.toString(key);
                Integer valueInteger = Bytes.toInt(value);

                metaData[i].map.put(keyString, valueInteger);
                metaData[i].rmap.put(valueInteger, keyString);
            });

        } catch (IOException e) {
            LOG.error("Could not scan table", e);
        }
    }

    /**
     * Get string-to-integer mapping for key
     *
     * @param keyStr    Key to lookup
     * @param metaIndex Metadata type
     * @return Integer mapping if exists, null otherwise
     */
    private Integer checkExistingMapping(String keyStr, HBaseMetadataIndex metaIndex) {

        int                           i    = metaIndex.getIndex();
        MetaData                      meta = metaData[i];
        HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];

        byte[] key          = Bytes.toBytes(keyStr);
        byte[] columnFamily = info.getColumnFamily();
        byte[] column       = info.getColumn();

        Get g = new Get(key);
        g.addColumn(columnFamily, column);

        try {
            Result result = meta.table.get(g);
            if (!result.isEmpty()) {
                byte[] columnValue = result.getValue(columnFamily, column);
                return Bytes.toInt(columnValue);
            }
        } catch (IOException e) {
            LOG.error("Could not get ref ", e);
        }

        return null;
    }

    /**
     * Create new string-to-integer mapping for metric key
     *
     * @param keyStr    key to map
     * @param metaIndex metadata type
     * @return New integer mapping if successful, null otherwise
     */
    private Integer insertNewMapping(String keyStr, HBaseMetadataIndex metaIndex) {

        int                           i    = metaIndex.getIndex();
        MetaData                      meta = metaData[i];
        HBaseSchema.MetadataTableInfo info = _schema.metadataTableInfos[i];

        byte[] key          = Bytes.toBytes(keyStr);
        byte[] columnFamily = info.getColumnFamily();
        byte[] column       = info.getColumn();
        byte[] refcounter   = info.getRefcounter();


        // Get current refcounter
        Integer counter;

        try {
            counter = (int) meta.table.incrementColumnValue(refcounter, columnFamily, column, 1);
        } catch (IOException e) {
            LOG.error("Could not get column ref", e);
            return null;
        }

        // Create new Put instance with ref obtained above
        try {
            Put    p     = new Put(key);
            byte[] value = Bytes.toBytes(counter);
            p.add(columnFamily, column, value);
            meta.table.put(p);
        } catch (IOException e) {
            LOG.error("Could not create mapping");
            return null;
        }

        return counter;

    }

    /**
     * Get integer mapping for given key. Create new mapping if it doesnt exist.
     *
     * @param metaIndex Metadata type
     * @param key       Key string
     * @return Integer mapping
     */
    private Integer getRef(HBaseMetadataIndex metaIndex, String key) {

        int      i    = metaIndex.getIndex();
        MetaData meta = metaData[i];

        Integer ref = meta.map.get(key);

        if (ref == null && key != null) {
            ref = checkExistingMapping(key, metaIndex);
            if (ref == null)
                ref = insertNewMapping(key, metaIndex);
            meta.map.put(key, ref);
            meta.rmap.put(ref, key);
        }

        return ref;
    }

    /**
     * Get string from integer from reverse maps
     *
     * @param metaIndex Metadata type
     * @param ref       Integer mapping
     * @return Key string
     * @implNote reloads entire map from store if mapping not found
     */
    private String getReverseRef(HBaseMetadataIndex metaIndex, Integer ref) {

        int      i    = metaIndex.getIndex();
        MetaData meta = metaData[i];

        if (!meta.rmap.containsKey(ref) && ref != null) {
            // reload map from store
            initializeMap(metaIndex);
        }

        return meta.rmap.get(ref);
    }

    /**
     * Create HBase Put operation from metric
     *
     * @param m Metric to insert
     * @return Put operation
     * @throws MetricException On Put creation failure
     */
    public abstract Put createPutOperation(Metric m) throws MetricException;

    /**
     * Create serialized key from metric
     *
     * @param m Metric to create key from
     * @return Serialized key
     * @throws MetricException On key creation failure, null metric entries
     */
    public byte[] createKey(Metric m) throws MetricException {
        Integer topoId       = getRef(TOPOLOGY, m.getTopoIdStr());
        Integer streamId     = getRef(STREAM, m.getStream());
        Integer hostId       = getRef(HOST, m.getHost());
        Integer compId       = getRef(COMP, m.getCompName());
        Integer metricNameId = getRef(METRICNAME, m.getMetricName());
        Integer executorId   = getRef(EXECUTOR, m.getExecutor());

        ByteBuffer bb = ByteBuffer.allocate(KEY_LENGTH);

        try {
            bb.put(m.getAggLevel());
            bb.putInt(topoId);
            bb.putInt(metricNameId);
            bb.putInt(compId);
            bb.putInt(executorId);
            bb.putInt(hostId);
            bb.putLong(m.getPort());
            bb.putInt(streamId);
        } catch (NullPointerException e) {
            throw new MetricException("Could not create metric key - null IDs", e);
        }

        int length = bb.position();
        bb.position(0);

        byte[] key = new byte[length];
        bb.get(key, 0, length);

        return key;
    }

    /**
     * Populate metric keys from HBase result
     *
     * @param m      metric to populate
     * @param result result from get or scan
     * @return whether metric was populated or not
     */
    public boolean populateMetricKey(Metric m, Result result) {

        byte[] key       = result.getRow();
        long   timeStamp = result.rawCells()[0].getTimestamp();

        ByteBuffer bb = ByteBuffer.allocate(KEY_LENGTH).put(key);
        bb.rewind();

        Byte    aggLevel     = bb.get();
        Integer topoId       = bb.getInt();
        Integer metricNameId = bb.getInt();
        Integer compId       = bb.getInt();
        Integer executorId   = bb.getInt();
        Integer hostId       = bb.getInt();
        long    port         = bb.getLong();
        Integer streamId     = bb.getInt();

        String topoIdStr     = getReverseRef(TOPOLOGY, topoId);
        String metricNameStr = getReverseRef(METRICNAME, metricNameId);
        String compIdStr     = getReverseRef(COMP, compId);
        String execIdStr     = getReverseRef(EXECUTOR, executorId);
        String hostIdStr     = getReverseRef(HOST, hostId);
        String streamIdStr   = getReverseRef(STREAM, streamId);

        m.setAggLevel(aggLevel);
        m.setTopoIdStr(topoIdStr);
        m.setTimeStamp(timeStamp);
        m.setMetricName(metricNameStr);
        m.setCompName(compIdStr);
        m.setExecutor(execIdStr);
        m.setHost(hostIdStr);
        m.setPort(port);
        m.setStream(streamIdStr);

        return true;
    }

    /**
     * Populate metric values from HBase result
     *
     * @param m      metric to populate
     * @param result result from get or scan
     * @return whether metric was populated or not
     */
    public abstract boolean populateMetricValue(Metric m, Result result);

    /**
     * Create scan operation from filters
     *
     * @param settings map containing settings to filter scan
     * @return List of unique scan operations
     * @see HBaseStoreScan
     */
    public List<Scan> createScanOperation(HashMap<String, Object> settings) {

        // grab values from map
        Integer         aggLevel     = (Integer) settings.get(StringKeywords.aggLevel);
        String          topoIdStr    = (String) settings.get(StringKeywords.topoId);
        String          compIdStr    = (String) settings.get(StringKeywords.component);
        String          execIdStr    = (String) settings.get(StringKeywords.executor);
        String          hostIdStr    = (String) settings.get(StringKeywords.host);
        String          portStr      = (String) settings.get(StringKeywords.port);
        String          streamIdStr  = (String) settings.get(StringKeywords.stream);
        HashSet<String> metricStrSet = (HashSet<String>) settings.get(StringKeywords.metricSet);
        Set<TimeRange>  timeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);

        // convert strings to Integer references
        Integer topoId   = getRef(TOPOLOGY, topoIdStr);
        Integer compId   = getRef(COMP, compIdStr);
        Integer execId   = getRef(EXECUTOR, execIdStr);
        Integer hostId   = getRef(HOST, hostIdStr);
        Integer streamId = getRef(STREAM, streamIdStr);
        Long    port     = (portStr == null) ? null : Long.parseLong(portStr);

        HashSet<Integer> metricIds = null;

        if (metricStrSet != null) {
            metricIds = new HashSet<>(metricStrSet.size());
            for (String s : metricStrSet) {
                Integer ref = getRef(METRICNAME, s);
                if (ref != null)
                    metricIds.add(ref);
                else
                    LOG.error("Could not lookup {} reference", s);
            }
        }

        HBaseStoreScan scan = new HBaseStoreScan()
                .withAggLevel(aggLevel)
                .withTopoId(topoId)
                .withTimeRange(timeRangeSet)
                .withMetricSet(metricIds)
                .withCompId(compId)
                .withExecutorId(execId)
                .withHostId(hostId)
                .withPort(port)
                .withStreamId(streamId);

        return scan.getScanList();
    }

}
