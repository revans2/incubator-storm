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


import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseSerializerExpanded extends HBaseSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseSerializerExpanded.class);

    public HBaseSerializerExpanded(HConnection hbaseConnection, HBaseSchema schema) {
        super(hbaseConnection, schema);
    }

    /**
     * Create HBase Put operation from metric
     *
     * @param m Metric to insert
     * @return Put operation
     * @throws MetricException On key creation failure
     */
    public Put createPutOperation(Metric m) throws MetricException {

        HBaseSchema.MetricsTableInfo info = _schema.metricsTableInfo;

        boolean isAggregate = m.getCount() > 1;

        long timestamp = m.getTimeStamp();
        byte[] key = createKey(m);
        byte[] value = Bytes.toBytes(m.getValue());
        byte[] count = Bytes.toBytes(m.getCount());
        byte[] columnFamily = info.getColumnFamily();

        Put p = new Put(key, timestamp);

        p.add(columnFamily, info.getValueColumn(), value);
        p.add(columnFamily, info.getCountColumn(), count);

        if (isAggregate) {
            byte[] sum = Bytes.toBytes(m.getSum());
            byte[] min = Bytes.toBytes(m.getMin());
            byte[] max = Bytes.toBytes(m.getMax());

            p.add(columnFamily, info.getSumColumn(), sum);
            p.add(columnFamily, info.getMinColumn(), min);
            p.add(columnFamily, info.getMaxColumn(), max);
        }

        return p;
    }

    /**
     * Populate metric values from HBase result
     *
     * @param m      metric to populate
     * @param result result from get or scan
     * @return whether metric was populated or not
     */
    public boolean populateMetricValue(Metric m, Result result) {

        HBaseSchema.MetricsTableInfo info = _schema.metricsTableInfo;
        byte[] cf = info.getColumnFamily();

        byte[] valueBytes = result.getValue(cf, info.getValueColumn());
        byte[] countBytes = result.getValue(cf, info.getCountColumn());
        byte[] sumBytes = result.getValue(cf, info.getSumColumn());
        byte[] minBytes = result.getValue(cf, info.getMinColumn());
        byte[] maxBytes = result.getValue(cf, info.getMaxColumn());

        try {
            double value = Bytes.toDouble(valueBytes);
            long count = Bytes.toLong(countBytes);

            m.setValue(value);
            m.setCount(count);
            if (count > 1) {
                double sum = Bytes.toDouble(sumBytes);
                double min = Bytes.toDouble(minBytes);
                double max = Bytes.toDouble(maxBytes);

                m.setSum(sum);
                m.setMin(min);
                m.setMax(max);
            }

            return true;
        } catch (NullPointerException e) {
            m.setValue(0.00);
            m.setCount(0L);
            return false;
        }
    }

}
