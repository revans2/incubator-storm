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

import java.nio.ByteBuffer;

public class HBaseSerializerCompact extends HBaseSerializer {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseSerializerCompact.class);

    private static final int VALUE_OFFSET = 0;
    private static final int COUNT_OFFSET = Double.BYTES;
    private static final int SUM_OFFSET = COUNT_OFFSET + Long.BYTES;
    private static final int MIN_OFFSET = SUM_OFFSET + Double.BYTES;
    private static final int MAX_OFFSET = MIN_OFFSET + Double.BYTES;
    private static final int MAX_VALUE_SIZE = MAX_OFFSET + Double.BYTES;

    public HBaseSerializerCompact(HConnection hbaseConnection, HBaseSchema schema) {
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

        long   timestamp    = m.getTimeStamp();
        byte[] key          = createKey(m);
        byte[] columnFamily = info.getColumnFamily();
        byte[] column       = info.getValueColumn();

        ByteBuffer bb = ByteBuffer.allocate(MAX_VALUE_SIZE);

        bb.putDouble(m.getValue());
        bb.putLong(m.getCount());
        if (isAggregate) {
            bb.putDouble(m.getSum());
            bb.putDouble(m.getMin());
            bb.putDouble(m.getMax());
        }

        int    bbLen = bb.position();
        byte[] value = new byte[bbLen];
        bb.rewind();
        bb.get(value, 0, bbLen);

        Put p = new Put(key, timestamp);
        p.add(columnFamily, column, value);

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

        HBaseSchema.MetricsTableInfo info       = _schema.metricsTableInfo;
        byte[]                       cf         = info.getColumnFamily();
        byte[]                       valueBytes = result.getValue(cf, info.getValueColumn());

        try {
            double value = Bytes.toDouble(valueBytes, VALUE_OFFSET);
            long   count = Bytes.toLong(valueBytes, COUNT_OFFSET);

            m.setValue(value);
            m.setCount(count);
            if (count > 1) {
                double sum = Bytes.toDouble(valueBytes, SUM_OFFSET);
                double min = Bytes.toDouble(valueBytes, MIN_OFFSET);
                double max = Bytes.toDouble(valueBytes, MAX_OFFSET);

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
