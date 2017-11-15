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


import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

class HBaseStoreScan {
    private final static Logger LOG = LoggerFactory.getLogger(HBaseStoreScan.class);

    private final static int PRE_START_OFFSET = 0;
    private final static int PRE_LENGTH = 5;
    private final static int POST_START_OFFSET = 9;
    private final static int POST_LENGTH = 24;
    private final static int METRIC_OFFSET = PRE_LENGTH;
    private final static int METRIC_LENGTH = 4;


    private ArrayList<Scan> scanList;
    private ByteBuffer pre;
    private ByteBuffer post;
    private int prefixLength;
    private HashSet<Integer> metricIds;
    private Set<TimeRange> timeRangeSet;

    HBaseStoreScan() {
        pre = ByteBuffer.allocate(PRE_LENGTH);
        post = ByteBuffer.allocate(POST_LENGTH);
        prefixLength = 0;
    }

    HBaseStoreScan withAggLevel(Integer aggLevel) {
        if (aggLevel != null) {
            pre.put(aggLevel.byteValue());
            prefixLength = 1;
        }
        return this;
    }

    HBaseStoreScan withTopoId(Integer topoId) {
        if (topoId != null) {
            pre.putInt(topoId);
            prefixLength = 5;
        }
        return this;
    }

    HBaseStoreScan withTimeRange(Set<TimeRange> timeRangeSet) {
        if (timeRangeSet != null) {
            this.timeRangeSet = timeRangeSet;
        }
        return this;
    }

    HBaseStoreScan withMetricSet(HashSet<Integer> metricIds) {
        if (metricIds != null && !metricIds.isEmpty()) {
            this.metricIds = metricIds;
            prefixLength = 9;
        }
        return this;
    }

    HBaseStoreScan withCompId(Integer compId) {
        if (compId != null) {
            post.putInt(compId);
            prefixLength = 13;
        }
        return this;
    }

    HBaseStoreScan withExecutorId(Integer executorId) {
        if (executorId != null) {
            post.putInt(executorId);
            prefixLength = 17;
        }
        return this;
    }

    HBaseStoreScan withHostId(Integer hostId) {
        if (hostId != null) {
            post.putInt(hostId);
            prefixLength = 21;
        }
        return this;
    }

    HBaseStoreScan withPort(Long port) {
        if (port != null) {
            post.putLong(port);
            prefixLength = 29;
        }
        return this;
    }

    HBaseStoreScan withStreamId(Integer streamId) {
        if (streamId != null) {
            post.putInt(streamId);
            prefixLength = 33;
        }
        return this;
    }

    List<Scan> getScanList() {
        if (scanList == null)
            generateScanList();
        return scanList;
    }

    private void generateScanList() {

        scanList = new ArrayList<>();

        // create buffer without metricId
        byte[] prefixArray = new byte[prefixLength];
        int    byteBufferLength;
        if (prefixLength > PRE_START_OFFSET) {
            byteBufferLength = pre.position();
            pre.position(0);
            pre.get(prefixArray, PRE_START_OFFSET, byteBufferLength);
        }
        if (prefixLength > POST_START_OFFSET) {
            byteBufferLength = post.position();
            post.position(0);
            post.get(prefixArray, POST_START_OFFSET, byteBufferLength);
        }

        byte[] metricBytes;
        if (metricIds != null) {
            for (Integer metricId : metricIds) {
                metricBytes = Bytes.toBytes(metricId);
                System.arraycopy(metricBytes, 0, prefixArray, METRIC_OFFSET, METRIC_LENGTH);
                createNewScans(prefixArray.clone());
            }
        } else {
            createNewScans(prefixArray.clone());
        }

    }

    private void createNewScans(byte[] prefix) {

        Scan s = new Scan().setMaxVersions();
        setRowPrefixFilter(s, prefix);

        if (timeRangeSet != null) {

            long start, end;
            for (TimeRange timeRange : timeRangeSet) {
                start = timeRange.startTime != null ? timeRange.startTime : 0L;
                end = timeRange.endTime != null ? timeRange.endTime : Long.MAX_VALUE - 1;

                if (end < start) {
                    LOG.warn("Malformed timerange = ({}, {}), skipping...", start, end);
                    continue;
                }

                try {
                    s.setTimeRange(start, end);
                    LOG.info("Creating scan with prefix {} between {} - {}", Bytes.toStringBinary(prefix), start, end);
                    scanList.add(new Scan(s));
                } catch (IOException e) {
                    LOG.error("Could not create scan min = {} max = {}", start, end, e);
                }
            }

        } else {
            LOG.info("Creating scan with prefix {}", Bytes.toStringBinary(prefix));
            scanList.add(s);
        }


    }

    /* from hbase 1.1.0 */
    private void setRowPrefixFilter(Scan s, byte[] rowPrefix) {
        if (rowPrefix == null) {
            s.setStartRow(HConstants.EMPTY_START_ROW);
            s.setStopRow(HConstants.EMPTY_END_ROW);
        } else {
            s.setStartRow(rowPrefix);
            s.setStopRow(calculateTheClosestNextRowKeyForPrefix(rowPrefix));
        }
    }

    private byte[] calculateTheClosestNextRowKeyForPrefix(byte[] rowKeyPrefix) {
        // Essentially we are treating it like an 'unsigned very very long' and doing +1 manually.
        // Search for the place where the trailing 0xFFs start
        int offset = rowKeyPrefix.length;
        while (offset > 0) {
            if (rowKeyPrefix[offset - 1] != (byte) 0xFF) {
                break;
            }
            offset--;
        }

        if (offset == 0) {
            // We got an 0xFFFF... (only FFs) stopRow value which is
            // the last possible prefix before the end of the table.
            // So set it to stop at the 'end of the table'
            return HConstants.EMPTY_END_ROW;
        }

        // Copy the right length of the original
        byte[] newStopRow = Arrays.copyOfRange(rowKeyPrefix, 0, offset);
        // And increment the last one
        newStopRow[newStopRow.length - 1]++;
        return newStopRow;
    }

}