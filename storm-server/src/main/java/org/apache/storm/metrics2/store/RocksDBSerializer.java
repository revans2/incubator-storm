/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;

import org.apache.commons.codec.binary.Hex;
import org.apache.storm.generated.StatsMetadata;
import org.apache.storm.generated.StatsMetadataTopo;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class RocksDBSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(RocksDBSerializer.class);

    // used to flag whether the serializer state has changed, and the store needs to
    // flush to RocksDB
    private boolean _hasChanged = false;

    // true if the serializer has been initialized
    private boolean _initialized = false;

    // thrift object that contains string->integer mapings for topology ids,
    // stream ids, and hosts
    private StatsMetadataTopo _metaTopos = new StatsMetadataTopo();

    // reverse maps for convenience to go from the numeric id, to the original value
    private Map<Integer, String> _revTopoIds;
    private Map<Integer, String> _revStreamIds;
    private Map<Integer, String> _revHostIds;

    // each topo gets a Metadata class. This has topo-specific information,
    // and is loaded/cached dynamically
    private Map<String, Metadata> metaInstances = new HashMap<String, Metadata>();

    public RocksDBSerializer() {
        _revTopoIds = new HashMap<Integer, String>();
        _revStreamIds = new HashMap<Integer, String>();
        _revHostIds = new HashMap<Integer, String>();

        _metaTopos.set_topo_ids(new HashMap<String, Integer>());
        _metaTopos.set_host_ids(new HashMap<String, Integer>());
        _metaTopos.set_stream_ids(new HashMap<String, Integer>());
    }

    public boolean isInitialized() {
        return _initialized;
    }

    public Set<String> getTopoIds() {
        return _metaTopos.get_topo_ids().keySet();
    }

    public Integer getTopoId(String topoId) {
        return fetchOrCreate(_metaTopos.get_topo_ids(), _revTopoIds, topoId);
    }

    public Integer getHostId(String hostId) {
        return fetchOrCreate(_metaTopos.get_host_ids(), _revHostIds, hostId);
    }

    public Integer getStreamId(String streamId) {
        return fetchOrCreate(_metaTopos.get_stream_ids(), _revStreamIds, streamId);
    }

    public String getTopo(Integer topoId) {
        return _revTopoIds.get(topoId);
    }

    public String getStream(Integer streamId) {
        return _revStreamIds.get(streamId);
    }

    public String getHost(Integer hostId) {
        return _revHostIds.get(hostId);
    }

    public Integer fetchOrCreate(Map<String, Integer> map, Map<Integer, String> rmap, String key) {
        if (map.containsKey(key)) {
            return map.get(key);
        }
        Integer num = map.size();
        map.put(key, num);
        rmap.put(num, key);
        _hasChanged = true;
        return num;
    }

    public class SerializationResult {
        // the key for the metric
        public byte[] metricKey;

        // the value for the metric
        public byte[] metricValue;

        // the overall metadata payload
        public byte[] metaValue;

        // the topology's metadata
        public byte[] metaTopoValue;
    }

    public SerializationResult serialize(Metric m) {
        SerializationResult sr = new SerializationResult();
        sr.metricKey = serializeMetricKey(m);
        sr.metricValue = serializeMetricValue(m);
        sr.metaValue = serializeMeta();
        sr.metaTopoValue = serializeMetaTopo(m);
        return sr;
    }


    private byte[] serializeMetricKey(Metric m) {
        byte[] keyBytes = null;

        String topoId = m.getTopoIdStr();

        // get the Metadata instance for the topo
        Metadata meta = getInstance(topoId);
        return meta.serializeKey(m);
    }

    private byte[] serializeMetricValue(Metric m) {
        byte[] dataBytes = null;

        String topoId = m.getTopoIdStr();

        // get the Metadata instance for the topo
        Metadata meta = getInstance(topoId);
        dataBytes = meta.serializeValue(m);
        return dataBytes;
    }

    private byte[] serializeMeta() {
        // get the Metadata instance for the topo
        byte[] metaTopoBytes = null;
        if (_hasChanged) {
            TSerializer ser = new TSerializer();
            try {
                metaTopoBytes = ser.serialize(_metaTopos);
                _hasChanged = false;
            } catch (TException ex) {
                LOG.error("Thrift exception", ex);
            }
        }
        return metaTopoBytes;
    }

    private byte[] serializeMetaTopo(Metric m) {
        // get the Metadata instance for the topo
        String topoId = m.getTopoIdStr();
        return getInstance(topoId).serialize();
    }

    public byte[] metadataKey(Metric m) {
        return metadataKey(getTopoId(m.getTopoIdStr()));

    }

    public boolean putToTopoMap(byte[] key, byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(key);
        //TODO: store these byte values here
        if ((byte) 0 != bb.get() ||
                (byte) 0 != bb.get()) { // second byte is metadata type
            return false;
        }
        //TODO: change thrift object so these are all longs
        //TODO: RHS can be thrift object
        Integer topoId = bb.getInt();
        try {
            String topoIdStr = new String(value, "UTF-8");
            _metaTopos.put_to_topo_ids(topoIdStr, topoId);
            _revTopoIds.put(topoId, topoIdStr);
        } catch (java.io.UnsupportedEncodingException ex) {
            LOG.error("Unsupoorted encoding!", ex);
            return false;
        }
        return true;
    }

    public boolean putToStreamMap(byte[] key, byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(key);
        if ((byte) 0 != bb.get() ||
                (byte) 1 != bb.get()) { // second byte is metadata type
            return false;
        }
        Integer streamId = bb.getInt();
        try {
            String streamIdStr = new String(value, "UTF-8");
            _metaTopos.put_to_stream_ids(streamIdStr, streamId);
            _revStreamIds.put(streamId, streamIdStr);
        } catch (java.io.UnsupportedEncodingException ex) {
            LOG.error("Unsupoorted encoding!", ex);
            return false;
        }
        return true;
    }

    public boolean putToHostMap(byte[] key, byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(key);
        if ((byte) 0 != bb.get() ||
                (byte) 2 != bb.get()) { // second byte is metadata type
            return false;
        }
        Integer hostId = bb.getInt();
        try {
            String host = new String(value, "UTF-8");
            _metaTopos.put_to_host_ids(host, hostId);
            _revHostIds.put(hostId, host);
        } catch (java.io.UnsupportedEncodingException ex) {
            LOG.error("Unsupoorted encoding!", ex);
            return false;
        }
        return true;
    }

    public byte[] makeKey(byte type, Integer key) {
        //TODO: size too big
        ByteBuffer bb = ByteBuffer.allocate(14);
        bb.put((byte) 0); // metadata
        bb.put(type);    // metadata type
        bb.putInt(key == null ? 0 : key);

        int length = bb.position();
        bb.position(0); //rewind
        byte[] result = new byte[length];
        bb.get(result, 0, length);
        return result;
    }

    private byte[] metadataKey(Integer topoId) {
        ByteBuffer bb = ByteBuffer.allocate(320);
        bb.put((byte) 0); // metadata
        bb.put((byte) 3); // topo metadata
        bb.putInt(topoId);
        bb.putLong(0);
        bb.putInt(0);
        bb.putInt(0);
        bb.putInt(0);
        bb.putInt(0);
        bb.putLong(0);
        bb.putInt(0);

        int length = bb.position();
        bb.position(0); //rewind
        byte[] result = new byte[length];
        bb.get(result, 0, length);
        return result;
    }

    public void populate(Metric m, byte[] valueInBytes) {
        if (valueInBytes == null) {
            m.setValue(0.0);
            m.setMin(0.0);
            m.setMax(0.0);
            m.setSum(0.0);
            m.setCount(0L);
            return;
        }
        ByteBuffer bb = ByteBuffer.wrap(valueInBytes);
        // count must be set after setValue()
        long count = bb.getLong();
        m.setValue(bb.getDouble());
        m.setMin(bb.getDouble());
        m.setMax(bb.getDouble());
        m.setSum(bb.getDouble());
        m.setCount(count);
    }

    public boolean metaInitialized(Metric m) {
        Metadata meta = getInstance(m.getTopoIdStr());
        return meta.initialized();
    }

    public void deserializeMeta(String topoId, byte[] data) {
        Metadata meta = getInstance(topoId);
        if (meta.initialized()) {
            return;
        }
        meta.deserialize(data);
    }

    public Metric deserialize(byte[] data) {
        // attempt to find the topoId, and in turn the Metadata instance
        // if not found in memory, seek out to RocksDBStore for the bytew 
        if (data != null) {
            ByteBuffer bb = ByteBuffer.wrap(data);

            byte type = bb.get(); // type (ignore)
            if (type != (byte) 1) {
                // type is metadata
                return null;
            }
            byte    aggLevel = bb.get(); // agg level, disregard
            Integer topoId   = bb.getInt();

            // popoulate if you haven't already
            Metadata meta = getInstance(topoId);
            if (!meta.initialized()) {
                meta.deserialize(data);
            }

            // deserialize actual metric
            RocksDBMetric metric = new RocksDBMetric(data);

            metric.setAggLevel(aggLevel);
            metric.setTopoId(topoId);
            metric.setTimeStamp(bb.getLong());
            metric.setMetricId(bb.getInt());
            metric.setCompId(bb.getInt());
            metric.setExecId(bb.getInt());
            metric.setHostId(bb.getInt());
            metric.setPort(bb.getLong());
            metric.setStreamId(bb.getInt());

            metric.setTopoIdStr(getTopo(topoId));
            metric.setMetricName(meta.getMetric(metric.getMetricId()));
            metric.setCompName(meta.getComp(metric.getCompId()));
            metric.setExecutor(meta.getExec(metric.getExecId()));
            metric.setHost(getHost(metric.getHostId()));
            metric.setStream(getStream(metric.getStreamId()));
            return metric;
        }
        return null;
    }

    public void deserializeTopoMap(byte[] data) {
        if (data != null) {
            TDeserializer dser = new TDeserializer();
            try {
                dser.deserialize(_metaTopos, data);
                // populate reverse collections
                populateTopoReverseCollection();
                LOG.debug("Metadata deserialized: {}", _metaTopos);
            } catch (TException ex) {
                LOG.error("Metadata failed to deserialize!", ex);
            }
        }
    }

    private void populateTopoReverseCollection() {
        populateReverseCollection(_metaTopos.get_topo_ids(), _revTopoIds);
        populateReverseCollection(_metaTopos.get_host_ids(), _revHostIds);
        populateReverseCollection(_metaTopos.get_stream_ids(), _revStreamIds);
    }

    public void populateReverseCollection(Map<String, Integer> map, Map<Integer, String> rmap) {
        for (Map.Entry<String, Integer> kv : map.entrySet()) {
            // build the reverse map
            rmap.put(kv.getValue(), kv.getKey());
        }
    }

    public Metadata getInstance(String topoIdStr) {
        Metadata m = metaInstances.get(topoIdStr);
        if (m == null) {
            m = new Metadata(topoIdStr, getTopoId(topoIdStr));
            metaInstances.put(topoIdStr, m);
        }
        return m;
    }

    public Metadata getInstance(Integer topoId) {
        String topoIdStr = _revTopoIds.get(topoId);
        return getInstance(topoIdStr);
    }

    public String contents() {
        return String.format(
                "topos: %s, rtopos: %s\n" +
                        "streamIds: %s, rstreamIds: %s\n" +
                        "hostIds: %s, rhostIds: %s\n",
                Arrays.toString(_metaTopos.get_topo_ids().entrySet().toArray()),
                Arrays.toString(_revTopoIds.entrySet().toArray()),

                Arrays.toString(_metaTopos.get_stream_ids().entrySet().toArray()),
                Arrays.toString(_revStreamIds.entrySet().toArray()),

                Arrays.toString(_metaTopos.get_host_ids().entrySet().toArray()),
                Arrays.toString(_revHostIds.entrySet().toArray()));
    }

    public byte[] createPrefix(Map<String, Object> settings) {
        String          topoIdStr   = (String) settings.get(StringKeywords.topoId);
        HashSet<String> metricIds   = (HashSet<String>) settings.get(StringKeywords.metricSet);
        String          metricIdStr = null;
        if (metricIds != null && metricIds.size() == 1) {
            metricIdStr = metricIds.iterator().next();
        }

        String  compIdStr   = (String) settings.get(StringKeywords.component);
        String  execIdStr   = (String) settings.get(StringKeywords.executor);
        String  host        = (String) settings.get(StringKeywords.host);
        String  port        = (String) settings.get(StringKeywords.port);
        String  stream      = (String) settings.get(StringKeywords.stream);
        Integer aggLevelInt = (Integer) settings.get(StringKeywords.aggLevel);
        Byte    aggLevel    = (aggLevelInt != null) ? aggLevelInt.byteValue() : (byte) 0;

        Metadata m = getInstance(topoIdStr);

        ByteBuffer bb = ByteBuffer.allocate(320);
        bb.put((byte) 1); // non metadata
        bb.put(aggLevel);
        Set<TimeRange> timeRangeSet = (Set<TimeRange>) settings.get(StringKeywords.timeRangeSet);
        Long           cur          = 0L;
        if (timeRangeSet != null) {
            Long minTime = null;
            for (TimeRange tr : timeRangeSet) {
                if (minTime == null || tr.startTime < minTime) {
                    minTime = tr.startTime;
                }
            }
            cur = minTime;
        }

        LOG.info("=> topo id str {}, topo id {}", topoIdStr, getTopoId(topoIdStr));
        bb.putInt(getTopoId(topoIdStr));
        bb.putLong(cur == null ? 0L : cur.longValue());
        bb.putInt(metricIdStr != null ? m.getMetricId(metricIdStr) : 0);
        bb.putInt(compIdStr != null ? m.getCompId(compIdStr) : 0);
        bb.putInt(execIdStr != null ? m.getExecId(execIdStr) : 0);
        bb.putInt(host != null ? getHostId(host) : 0);
        bb.putLong(port != null ? Long.parseLong(port) : 0L);
        bb.putInt(stream != null ? getStreamId(stream) : 0);

        int length = bb.position();
        bb.position(0); // go to beginning
        byte[] result = new byte[length];
        bb.get(result, 0, length); // copy to position

        LOG.info("Creating prefix for {}, {} {} {} {} {} {} {} {} {}\n => {}\nMeta: \n{}",
                cur, aggLevel, topoIdStr, metricIds, metricIdStr, compIdStr, execIdStr, host, port, stream, Hex.encodeHexString(result), m);

        return result;
    }

    private class Metadata {
        private Integer _topoId;
        private StatsMetadata _meta;
        private Map<Integer, String> _revCompIds;
        private Map<Integer, String> _revMetricIds;

        private Map<Integer, String> _revExecutorIds;
        private boolean _hasChanged = false;
        private boolean _initialized = false;

        public Metadata(String topoIdStr, Integer topoId) {
            // reverse colletions to find the string ids in constant time
            _meta = new StatsMetadata();
            _topoId = topoId;
            _revCompIds = new HashMap<Integer, String>();
            _revMetricIds = new HashMap<Integer, String>();
            _revExecutorIds = new HashMap<Integer, String>();

            _meta.set_comp_ids(new HashMap<String, Integer>());
            _meta.set_metric_ids(new HashMap<String, Integer>());
            _meta.set_executor_ids(new HashMap<String, Integer>());
        }

        public boolean initialized() {
            return _initialized;
        }

        public void deserialize(byte[] data) {
            if (data != null) {
                LOG.info("Loading meta!");
                TDeserializer dser = new TDeserializer();
                try {
                    dser.deserialize(_meta, data);
                    // populate reverse collections
                    populateReverseCollections();
                    _initialized = true;
                    LOG.info("Topology metadata deserialized: {}", _meta);
                } catch (TException ex) {
                    //TODO: log it?
                }
            }
        }

        public byte[] serializeKey(Metric m) {
            ByteBuffer bb = ByteBuffer.allocate(320);
            bb.put((byte) 1); // non metadata
            bb.put(m.getAggLevel());
            bb.putInt(_topoId);
            bb.putLong(m.getTimeStamp());
            bb.putInt(getMetricId(m.getMetricName()));
            bb.putInt(getCompId(m.getCompName()));
            bb.putInt(getExecId(m.getExecutor()));
            bb.putInt(getHostId(m.getHost()));
            bb.putLong(m.getPort());
            bb.putInt(getStreamId(m.getStream()));

            int length = bb.position();
            bb.position(0); //rewind
            byte[] result = new byte[length];
            bb.get(result, 0, length);
            return result;
        }

        public byte[] serializeValue(Metric m) {
            int        bufferSize = m.getCount() > 1 ? 320 : 128;
            ByteBuffer bb         = ByteBuffer.allocate(bufferSize);
            bb.putLong(m.getCount());
            bb.putDouble(m.getValue());
            bb.putDouble(m.getMin());
            bb.putDouble(m.getMax());
            bb.putDouble(m.getSum());

            int length = bb.position();
            bb.position(0); //rewind
            byte[] result = new byte[length];
            bb.get(result, 0, length);
            return result;
        }

        private void populateReverseCollections() {
            populateReverseCollection(_meta.get_comp_ids(), _revCompIds);
            populateReverseCollection(_meta.get_metric_ids(), _revMetricIds);
            populateReverseCollection(_meta.get_executor_ids(), _revExecutorIds);
        }

        public byte[] serialize() {
            if (_hasChanged) {
                TSerializer ser = new TSerializer();
                try {
                    byte[] result = ser.serialize(_meta);
                    _hasChanged = false;
                    LOG.info("Topology metadata serialized: {}", _meta);
                    return result;
                } catch (TException ex) {
                    //TODO: log it?
                    return null;
                }

            }
            return null;
        }

        public Integer getCompId(String compId) {
            return fetchOrCreate(_meta.get_comp_ids(), _revCompIds, compId);
        }

        public Integer getMetricId(String metricId) {
            return fetchOrCreate(_meta.get_metric_ids(), _revMetricIds, metricId);
        }

        public Integer getExecId(String executorId) {
            return fetchOrCreate(_meta.get_executor_ids(), _revExecutorIds, executorId);
        }

        public Integer fetchOrCreate(Map<String, Integer> map, Map<Integer, String> rmap, String key) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
            Integer num = map.size();
            map.put(key, num);
            rmap.put(num, key);
            _hasChanged = true;
            return num;
        }

        public String getComp(Integer compId) {
            return _revCompIds.get(compId);
        }

        public String getMetric(Integer metricId) {
            return _revMetricIds.get(metricId);
        }

        public String getExec(Integer executorId) {
            return _revExecutorIds.get(executorId);
        }
    }
}
