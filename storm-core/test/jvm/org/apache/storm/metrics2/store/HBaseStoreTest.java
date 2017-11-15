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


import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.storm.generated.Window;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class HBaseStoreTest {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseStoreTest.class);

    private static HBaseStore store;
    private static HashMap<String, Object> conf;
    private static HBaseTestingUtility testUtil;
    private static HTableInterface metricsTable;
    private static Random random = new Random();

    private static String currentStoreType = null;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"expanded"}, {"compact"}
        });
    }

    /**
     * Hacky constructor to reinit store when parameter changes
     * '@BeforeParam' in newer versions of Junit should replace this
     *
     * @param schemaType schema for HBaseStore
     */
    public HBaseStoreTest(String schemaType) {

        if (currentStoreType == null)
            currentStoreType = schemaType;

        if (!schemaType.equals(currentStoreType)) {
            // new param, reinit HBaseStore
            currentStoreType = schemaType;
            conf = makeConfig(schemaType);
            int zkPort = testUtil.getZkCluster().getClientPort();
            conf.put("HBaseZookeeperPortOverride", zkPort);
            LOG.info("ZKPort is {}", zkPort);

            try {
                testUtil.compact(true);
                initSchema(conf, testUtil.getHBaseAdmin());
                store = new HBaseStore().prepare(conf);
                metricsTable = store.getMetricsTable();
            } catch (Exception e) {
                fail("Unexpected exception - " + e);
            }

            LOG.info("Switching schema to {}", schemaType);
        }
    }

    private static HashMap<String, Object> makeConfig(String schemaType) {

        if (schemaType == null) {
            schemaType = "expanded";
        }

        HashMap<String, Object> confMap = new HashMap<>();
        // metadata map
        HashMap<String, Object> metaDataMap = new HashMap<>();
        List<String> metadataNames = Arrays.asList("topoMap", "streamMap", "hostMap",
                "compMap", "metricMap", "executorMap");

        metadataNames.forEach((name) -> {
            HashMap<String, String> m = new HashMap<>();
            m.put("name", "metadata");
            m.put("cf", "m");
            m.put("column", "c");
            m.put("refcounter", "REFCOUNTER");
            metaDataMap.put(name, m);
        });

        // columns map & metrics map
        HashMap<String, String> columnsMap = new HashMap<>();
        columnsMap.put("value", "v");
        columnsMap.put("sum", "s");
        columnsMap.put("count", "c");
        columnsMap.put("min", "i");
        columnsMap.put("max", "a");

        HashMap<String, Object> metricsMap = new HashMap<>();
        metricsMap.put("name", "metrics");
        metricsMap.put("cf", "c");
        if (schemaType.equals("compact"))
            metricsMap.put("column", "c");
        else if (schemaType.equals("expanded"))
            metricsMap.put("columns", columnsMap);

        // schema map
        HashMap<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("type", schemaType);
        schemaMap.put("metrics", metricsMap);
        schemaMap.put("metadata", metaDataMap);

        confMap.put("storm.metrics2.store.HBaseStore.hbase.schema", schemaMap);

        return confMap;
    }

    private static Metric makeMetric(long ts) {

        Metric m = new Metric("testMetric", ts,
                "testExecutor",
                "testComp",
                "testStream" + String.valueOf(ts),
                "testTopo",
                123.45);
        m.setHost("testHost");
        m.setValue(123.45);
        return m;
    }

    private static Metric makeAggMetric(long ts) {

        Metric m = makeMetric(ts);
        m.setAggLevel((byte) 1);
        m.setValue(100.0);

        for (int i = 1; i < 10; ++i) {
            m.updateAverage(100.00 * i);
        }

        return m;
    }

    private static void enableMetricsTable() {
        try {
            TableName name = metricsTable.getName();
            HBaseAdmin admin = testUtil.getHBaseAdmin();
            admin.enableTable(name);
        } catch (IOException e) {
            fail();
        }
    }

    private static void disableMetricsTable() {
        try {
            TableName name = metricsTable.getName();
            HBaseAdmin admin = testUtil.getHBaseAdmin();
            admin.disableTable(name);
        } catch (IOException e) {
            fail();
        }
    }

    private int[] generateTimestamps(int start, int end, int count) {
        if (end == 0 || end < start)
            return random.ints(start, count).distinct().limit(count).toArray();

        return random.ints(start, end).distinct().limit(count).toArray();
    }

    @BeforeClass
    public static void setUp() {

        conf = makeConfig(null);
        testUtil = new HBaseTestingUtility();

        try {

            testUtil.startMiniCluster();
            initSchema(conf, testUtil.getHBaseAdmin());

            // set ZK info from test cluster
            int zkPort = testUtil.getZkCluster().getClientPort();
            conf.put("HBaseZookeeperPortOverride", zkPort);

            store = new HBaseStore().prepare(conf);
            metricsTable = store.getMetricsTable();

        } catch (Exception e) {
            fail("Unexpected exception - " + e);
        }
    }

    private static void initSchema(Map conf, HBaseAdmin admin) {

        try {
            HBaseSchema schema = new HBaseSchema(conf);
            HashMap<TableName, ArrayList<HColumnDescriptor>> tableMap = schema.getTableMap();

            for (Map.Entry<TableName, ArrayList<HColumnDescriptor>> entry : tableMap.entrySet()) {

                TableName name = entry.getKey();
                List<HColumnDescriptor> columnsList = entry.getValue();

                HTableDescriptor descriptor = new HTableDescriptor(name);
                for (HColumnDescriptor columnDescriptor : columnsList) {
                    columnDescriptor.setMaxVersions(HConstants.ALL_VERSIONS);
                    descriptor.addFamily(columnDescriptor);
                }

                // disable and delete table if it exists
                if (admin.tableExists(name)) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
                // create table
                admin.createTable(descriptor);
            }
        } catch (Exception e) {
            fail("Unexpected exception - " + e);
        }
    }

    @After
    public void clearAndSleep() {

        // clear out metrics table
        ArrayList<Row> metricsToDelete = new ArrayList<>();
        try {
            Scan scanAll = new Scan();
            ResultScanner resultScanner = metricsTable.getScanner(scanAll);

            for (Result result : resultScanner) {
                Delete d = new Delete(result.getRow());
                metricsToDelete.add(d);
            }
            Object[] deleteResults = new Object[metricsToDelete.size()];
            metricsTable.batch(metricsToDelete, deleteResults);

            resultScanner.close();
        } catch (Exception e) {
            fail("Unexpected exception - " + e);
        }

        // add an artificial delay to avoid comodification errors
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail("Unexpected exception - " + e);
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            testUtil.shutdownMiniCluster();
        } catch (Exception e) {
            fail("Unexpected - " + e);
        }
    }

    @Test
    public void testInsert() {
        Metric m = makeMetric(random.nextInt(9999));
        store.insert(m);

        ResultScanner scanner;
        Scan s = new Scan();
        try {
            scanner = metricsTable.getScanner(s);
        } catch (Exception e) {
            fail("Unexpected - " + e);
            return;
        }

        for (Result result : scanner) {
            long ts = result.rawCells()[0].getTimestamp();
            if (m.getTimeStamp() == ts) {
                return;
            }
        }

        fail("Could not find metric in store.");

    }

    @Test
    public void testInsertFailures() {
        HTableInterface mockTable = mock(HTableInterface.class);
        Metric m = makeMetric(random.nextInt(9999));
        HBaseStore tempStore = new HBaseStore();

        // set up temp store with mock table
        try {
            tempStore.prepare(conf);
            tempStore.setMetricsTable(mockTable);
        } catch (MetricException e) {
            fail();
        }

        // try inserting malformed metric
        m.setTopoIdStr(null);
        tempStore.insert(m);
        verifyZeroInteractions(mockTable);

        // try inserting metric to unavailable table/server
        disableMetricsTable();
        m.setTopoIdStr("someTopology");
        tempStore.insert(m);
        try {
            verify(mockTable).put(isA(Put.class));
        } catch (IOException e) {
            fail("Unexpected exception - " + e.toString());
        }

        enableMetricsTable();

    }

    @Test
    public void testInsertAgg() {
        Metric m = makeAggMetric(random.nextInt(9999));
        store.insert(m);

        ResultScanner scanner;
        Scan s = new Scan();
        try {
            scanner = metricsTable.getScanner(s);
        } catch (Exception e) {
            fail("Unexpected - " + e);
            return;
        }

        for (Result result : scanner) {
            long ts = result.rawCells()[0].getTimestamp();
            if (m.getTimeStamp() == ts) {
                return;
            }
        }

        fail("Could not find metric in store.");
    }

    @Test
    public void testScan() {
        final int COUNT = 10;
        int[] timestamps = generateTimestamps(12000, 18000, COUNT);

        ArrayList<Metric> metricsList = new ArrayList<>(COUNT);
        ArrayList<Metric> retrievedMetrics = new ArrayList<>(COUNT);

        for (int i = 0; i < 10; i++) {
            Metric m = makeMetric(timestamps[i]);
            metricsList.add(m);
            store.insert(m);
        }

        store.scan((metric, timeRanges) -> retrievedMetrics.add(metric));

        assertTrue(retrievedMetrics.containsAll(metricsList));

    }

    @Test
    public void testFilteredScan() {

        final int COUNT = 100;

        ArrayList<Metric> metricsList = new ArrayList<>(COUNT);
        ArrayList<Metric> retrievedMetrics = new ArrayList<>(COUNT);
        int[] timestamps = generateTimestamps(0, 10000, COUNT);

        for (int i = 0; i < COUNT; i++) {
            Metric m = makeMetric(timestamps[i]);
            metricsList.add(m);
            store.insert(m);
        }

        Integer aggLevel = metricsList.get(0).getAggLevel().intValue();
        String topoIdStr = metricsList.get(0).getTopoIdStr();

        HashSet<TimeRange> timeRangeSet = new HashSet<>();
        timeRangeSet.add(new TimeRange(0L, 5000L, Window.ALL));
        timeRangeSet.add(new TimeRange(5000L, 10000L, Window.ALL));

        HashSet<String> metricsSet = new HashSet<>();
        metricsSet.add(metricsList.get(0).getMetricName());

        String compId = metricsList.get(0).getCompName();
        String executorId = metricsList.get(0).getExecutor();
        String hostId = metricsList.get(0).getHost();
        String port = String.valueOf(metricsList.get(0).getPort());

        LinkedHashMap<String, Object> filterMap = new LinkedHashMap<>();
        filterMap.put(StringKeywords.aggLevel, aggLevel);
        filterMap.put(StringKeywords.topoId, topoIdStr);
        filterMap.put(StringKeywords.timeRangeSet, timeRangeSet);
        filterMap.put(StringKeywords.metricSet, metricsSet);
        filterMap.put(StringKeywords.component, compId);
        filterMap.put(StringKeywords.executor, executorId);
        filterMap.put(StringKeywords.host, hostId);
        filterMap.put(StringKeywords.port, port);

        HashMap<String, Object> settings = new HashMap<>();
        // place initial timerangeset, not used until timeRangeSet is present
        settings.put(StringKeywords.initialTimeRangeSet, timeRangeSet);

        filterMap.forEach((key, value) -> {
            retrievedMetrics.clear();
            settings.put(key, value);
            store.scan(settings, (metric, timeRanges) -> {
                retrievedMetrics.add(metric);
                if (settings.containsKey(StringKeywords.timeRangeSet)) {
                    assertTrue(timeRangeSet.containsAll(timeRanges));
                }
            });
            assertTrue(retrievedMetrics.containsAll(metricsList));
            assertTrue(retrievedMetrics.size() >= COUNT);

        });

    }

    @Test
    public void testVersioning() {

        final int COUNT = 100;
        Metric m = makeMetric(25000);
        m.setStream("testStream"); // remove ts from stream

        for (int i = 1; i <= COUNT; ++i) {
            Metric ins = new Metric(m);
            ins.setTimeStamp(i * 25000L);
            store.insert(ins);
        }

        HashMap<String, Object> settings = new HashMap<>();
        settings.put(StringKeywords.aggLevel, m.getAggLevel().intValue());
        settings.put(StringKeywords.topoId, m.getTopoIdStr());

        TimeRange tr = new TimeRange(25000L, (25000L * COUNT) + 1L, Window.ALL);
        HashSet<TimeRange> timeSet = new HashSet<>();
        timeSet.add(tr);
        settings.put(StringKeywords.timeRangeSet, timeSet);
        settings.put(StringKeywords.initialTimeRangeSet, timeSet);

        ArrayList<Metric> metricsRetrieved = new ArrayList<>(COUNT);

        store.scan(settings, (metric, timeranges) -> {
            metricsRetrieved.add(metric);
            assertEquals(0, metric.getTimeStamp() % 25000);
        });

        assertEquals(COUNT, metricsRetrieved.size());

    }

    @Test
    public void testPopulateValue() {
        long ts = random.nextInt(99999);
        Metric m = makeMetric(ts);
        store.insert(m);

        Metric newMetric = makeMetric(ts);
        newMetric.setValue(0.00);
        newMetric.setCount(0L);

        // check that newMetric has values populated from inserted metric
        store.populateValue(newMetric);
        assertEquals(m.getCount(), newMetric.getCount());
        assertEquals(m.getValue(), newMetric.getValue(), 0.00001);

        // check that invalid new metric isn't populated
        newMetric.setTopoIdStr("BAD TOPOLOGY");
        newMetric.setValue(0.00);
        newMetric.setCount(0L);
        store.populateValue(newMetric);
        assertEquals(0L, newMetric.getCount());
        assertEquals(0.00, newMetric.getValue(), 0.00001);
        assertEquals(0.00, newMetric.getSum(), 0.00001);
        assertEquals(0.00, newMetric.getMin(), 0.00001);
        assertEquals(0.00, newMetric.getMax(), 0.00001);

    }

    @Test
    public void testPopulateValueAgg() {
        long ts = random.nextInt(99999);
        Metric m = makeAggMetric(ts);
        store.insert(m);

        Metric newMetric = makeAggMetric(ts);
        newMetric.setValue(0.00);
        newMetric.setCount(0L);

        // check that newMetric has values populated from inserted metric
        store.populateValue(newMetric);
        assertEquals(m.getCount(), newMetric.getCount());
        assertEquals(m.getValue(), newMetric.getValue(), 0.00001);
        assertEquals(m.getSum(), newMetric.getSum(), 0.00001);
        assertEquals(m.getMin(), newMetric.getMin(), 0.00001);
        assertEquals(m.getMax(), newMetric.getMax(), 0.00001);

        // check that invalid new metric isn't populated
        newMetric.setTopoIdStr("BAD TOPOLOGY");
        newMetric.setValue(0.00);
        newMetric.setCount(0L);
        store.populateValue(newMetric);
        assertEquals(0L, newMetric.getCount());
        assertEquals(0.00, newMetric.getValue(), 0.00001);
        assertEquals(0.00, newMetric.getSum(), 0.00001);
        assertEquals(0.00, newMetric.getMin(), 0.00001);
        assertEquals(0.00, newMetric.getMax(), 0.00001);
    }

    @Test
    public void testRemove() {

        ArrayList<Metric> insertedMetrics = new ArrayList<>();
        ArrayList<Metric> retrievedMetrics = new ArrayList<>();

        for (int i = 1; i <= 10; ++i) {
            Metric m = makeMetric(i);
            store.insert(m);
            insertedMetrics.add(m);
        }
        for (int i = 101; i <= 110; ++i) {
            Metric m = makeMetric(i);
            store.insert(m);
            insertedMetrics.add(m);
        }

        HashMap<String, Object> settings = new HashMap<>();
        HashSet<TimeRange> timeRangeSet = new HashSet<>();
        timeRangeSet.add(new TimeRange(1L, 10L + 1L, Window.ALL));
        timeRangeSet.add(new TimeRange(101L, 110L + 1L, Window.ALL));

        settings.put(StringKeywords.aggLevel, insertedMetrics.get(0).getAggLevel().intValue());
        settings.put(StringKeywords.topoId, insertedMetrics.get(0).getTopoIdStr());
        settings.put(StringKeywords.timeRangeSet, timeRangeSet);
        settings.put(StringKeywords.initialTimeRangeSet, timeRangeSet);

        // remove metrics
        store.remove(settings);

        // scan again, should have nil
        store.scan(settings, (metric, timeRanges) -> retrievedMetrics.add(metric));

        assertEquals(0, retrievedMetrics.size());

    }

    /**
     * Insert from one store and scan with another (and vice-versa)
     * Covers parts of code untouched in other tests
     */
    @Test
    public void testConcurrentStores() {

        final int COUNT = 50;
        HBaseStore otherStore;
        try {
            otherStore = new HBaseStore().prepare(conf);
        } catch (Exception e) {
            fail("Unexpected exception - " + e);
            return;
        }

        int[] timestamps = generateTimestamps(100000, 110000, COUNT);
        int[] timestampsOther = generateTimestamps(110000, 120000, COUNT);

        ArrayList<Metric> metricsInserted = new ArrayList<>(COUNT);
        ArrayList<Metric> metricsInsertedOther = new ArrayList<>(COUNT);
        ArrayList<Metric> metricsRetrieved = new ArrayList<>(COUNT);
        ArrayList<Metric> metricsRetrievedOther = new ArrayList<>(COUNT);

        HashSet<TimeRange> timeSet = new HashSet<>();
        timeSet.add(new TimeRange(100000L, 110000L, Window.ALL));

        HashSet<TimeRange> timeSetOther = new HashSet<>();
        timeSetOther.add(new TimeRange(110000L, 120000L, Window.ALL));

        HashMap<String, Object> settings = new HashMap<>();
        HashMap<String, Object> settingsOther = new HashMap<>();

        // insert with both stores
        for (int i = 0; i < COUNT; ++i) {
            Metric m1 = makeMetric(timestamps[i]);
            store.insert(m1);
            metricsInserted.add(m1);

            Metric m2 = makeMetric(timestampsOther[i]);
            otherStore.insert(m2);
            metricsInsertedOther.add(m2);
        }

        // scan other's metrics

        settings.put(StringKeywords.aggLevel, metricsInsertedOther.get(0).getAggLevel().intValue());
        settings.put(StringKeywords.topoId, metricsInsertedOther.get(0).getTopoIdStr());
        settings.put(StringKeywords.timeRangeSet, timeSetOther);
        settings.put(StringKeywords.initialTimeRangeSet, timeSetOther);

        store.scan(settings, (metric, timeranges) -> metricsRetrieved.add(metric));
        assertTrue(metricsRetrieved.containsAll(metricsInsertedOther));


        settingsOther.put(StringKeywords.aggLevel, metricsInserted.get(0).getAggLevel().intValue());
        settingsOther.put(StringKeywords.topoId, metricsInserted.get(0).getTopoIdStr());
        settingsOther.put(StringKeywords.timeRangeSet, timeSet);
        settingsOther.put(StringKeywords.initialTimeRangeSet, timeSet);

        store.scan(settingsOther, (metric, timeranges) -> metricsRetrievedOther.add(metric));
        assertTrue(metricsRetrievedOther.containsAll(metricsInserted));

    }

}