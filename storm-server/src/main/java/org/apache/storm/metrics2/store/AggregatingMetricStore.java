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

import org.apache.storm.generated.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AggregatingMetricStore implements MetricStore {
    private final static Logger LOG = LoggerFactory.getLogger(AggregatingMetricStore.class);
    private MetricStore store;

    private List<Long> _buckets;

    private class BucketInfo {
        // NOTE: roundedEndTime <= endTime && roundedStartTime <= startTime
        // for non-negative start and end times
        long startTime;
        long endTime;
        long roundedStartTime;
        long roundedEndTime;

        /**
         * Calculates rounded start and end times
         * @param t Timerange
         * @param resolution resolution (bucket value)
         */
        BucketInfo(TimeRange t, long resolution) {
            startTime = (t.startTime != null) ? t.startTime : 0L;
            endTime = (t.endTime != null) ? t.endTime : System.currentTimeMillis();
            roundedEndTime = resolution * (endTime / resolution);
            roundedStartTime = resolution * ((startTime + (resolution - 1L)) / resolution);
        }

    }

    // testing
    public List<Long> getBuckets() {
        return _buckets;
    }

    public MetricStore getUnderlyingStore() {
        return store;
    }
    // end testing

    /**
     * Constructor for AggregatingMetricStore
     * @param store Underlying store to perform queries on
     */
    public AggregatingMetricStore(MetricStore store) {
        this.store = store;
        _buckets = new ArrayList<>();
        _buckets.add(60L); // 60 minutes
        _buckets.add(10L); // 10 minutes
        _buckets.add(1L);  // 1 minutes
    }

    /**
     * Prepares store
     * @param config Storm config map
     * @return this for invocation chaining
     */
    @Override
    public AggregatingMetricStore prepare(Map config) {
        return this;
    }

    /**
     * Inserts raw metric and updates its corresponding aggregations for
     * each bucket
     * @param metric Metric to store
     */
    @Override
    public void insert(Metric metric) {

        LOG.debug("Inserting {}", metric);
        store.insert(metric);

        // update aggregates for each bucket
        for (Long bucket : _buckets) {
            updateAggregate(metric, bucket);
        }
    }

    /**
     * Updates aggregation metrics if it exists, else creates new agg metric
     * @param m Raw metric to update aggregates for
     * @param bucket Bucket to update for
     */
    private void updateAggregate(Metric m, Long bucket) {

        Metric aggMetric       = new Metric(m);
        Long   metricTimestamp = aggMetric.getTimeStamp();
        Double metricValue     = aggMetric.getValue();

        long msToBucket      = 1000 * 60 * bucket;
        Long roundedToBucket = msToBucket * (metricTimestamp / msToBucket);

        // set new key
        aggMetric.setAggLevel(bucket.byteValue());
        aggMetric.setTimeStamp(roundedToBucket);

        // retrieve existing aggregation
        if (store.populateValue(aggMetric))
            aggMetric.updateAverage(metricValue);
        else
            aggMetric.setValue(metricValue);

        // insert updated metric
        LOG.debug("inserting {} min bucket {}", aggMetric, bucket);
        store.insert(aggMetric);
    }

    /**
     * Scans underlying store
     * @param agg Aggregator
     */
    @Override
    public void scan(IAggregator agg) {
        store.scan(agg);
    }

    /**
     * Retrieves bucket for given index. However, if bucket is larger
     * than given timerange, returns next bucket lesser than timerange
     * @param i Index for _buckets
     * @param t Timerange for scan
     * @return Value for appropriate bucket
     */
    private Long getBucket(int i, TimeRange t) {

        Long res;
        long startTime = (t.startTime != null) ? t.startTime : 0L;
        long endTime   = (t.endTime != null) ? t.endTime : System.currentTimeMillis();
        long timeDelta = endTime - startTime;

        do {
            res = (i < _buckets.size()) ? _buckets.get(i) : 0L;
            ++i;
        } while (res * 60L * 1000L > timeDelta && timeDelta > 0);

        return res;
    }

    /**
     * Scans underlying store by dividing timerange into segments and querying at largest possible
     * resolution. Recursive calls on head and tail of divided timerange with smaller resolutions.
     * @param settings Map of settings to filter by
     * @param agg Aggregator
     * @param t Timerange to scan within
     * @param bucketsIdx Index of bucket to scan with (determines resolution)
     */
    private void _scan(HashMap<String, Object> settings, IAggregator agg, TimeRange t, int bucketsIdx) {

        Long               res     = getBucket(bucketsIdx, t);
        HashSet<TimeRange> timeSet = new HashSet<>();

        LOG.info("At _scan buckets with {} {} {} {}", settings, agg, t, res);

        if (res == 0) {
            timeSet.add(t);
            settings.put(StringKeywords.timeRangeSet, timeSet);
            settings.put(StringKeywords.aggLevel, 0);
            store.scan(settings, agg);
        } else {

            long       resMs      = 1000L * 60L * res;
            BucketInfo bucketInfo = new BucketInfo(t, resMs);

            // can the head be subdivided?
            if (bucketInfo.startTime != bucketInfo.roundedStartTime) {
                TimeRange thead = new TimeRange(bucketInfo.startTime, bucketInfo.roundedStartTime, t.window);
                _scan(settings, agg, thead, bucketsIdx + 1);
            }

            // did we find buckets for the body? If so, go ahead and scan
            if (t.startTime <= t.endTime) {
                TimeRange tbody = new TimeRange(bucketInfo.roundedStartTime, bucketInfo.roundedEndTime, t.window);
                timeSet.add(tbody);
                settings.put(StringKeywords.timeRangeSet, timeSet);
                settings.put(StringKeywords.aggLevel, res.intValue());
                store.scan(settings, agg);
            }

            // can the tail be subdivided?
            if (bucketInfo.roundedEndTime != bucketInfo.endTime) {
                TimeRange ttail = new TimeRange(bucketInfo.roundedEndTime, bucketInfo.endTime, t.window);
                _scan(settings, agg, ttail, bucketsIdx + 1);
            }
        }

    }

    /**
     * Initial scan method
     * @param settings Map of settings to filter by
     * @param agg IAggregator
     */
    @Override
    public void scan(HashMap<String, Object> settings, IAggregator agg) {
        HashSet<TimeRange> timeRangeSet = (HashSet<TimeRange>) settings.get(StringKeywords.timeRangeSet);

        if (timeRangeSet == null) {
            timeRangeSet = new HashSet<>();
            timeRangeSet.add(new TimeRange(0L, System.currentTimeMillis(), Window.ALL));
            settings.put(StringKeywords.timeRangeSet, timeRangeSet);
        }

        // preserve original time range set under new key
        settings.put(StringKeywords.initialTimeRangeSet, timeRangeSet);

        for (TimeRange t : timeRangeSet) {
            HashMap<String, Object> settingsCopy = new HashMap<>(settings);
            _scan(settingsCopy, agg, t, 0);
        }
    }

    /**
     * Calls remove on underlying store
     * @param settings Map of settings to filter by
     */
    @Override
    public void remove(HashMap<String, Object> settings) {
        store.remove(settings);
    }

    /**
     * Calls populateValue on underlying store
     * @param metric Metric to populate
     * @return whether metric was populated or not
     */
    @Override
    public boolean populateValue(Metric metric) {
        return store.populateValue(metric);
    }
}
