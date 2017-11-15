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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Metric {
    private final static Logger LOG = LoggerFactory.getLogger(Metric.class);

    private String owner;
    private String metricName;
    private String topoIdStr;
    private String host;
    private long port = 0;
    private String compIdStr;
    private long timestamp = 0;
    private String executor;
    private String stream;

    private long count = 1L;
    private double value = 0.0;
    private double sum = 0.0;
    private double min = 0.0;
    private double max = 0.0;
    private Byte aggLevel = (byte) 0; // raw values are not aggregated

    public Metric() {
    }

    public Metric(String metric, Long timestamp, String executor, String compId,
                  String stream, String topoIdStr, Double value) {
        this.metricName = metric;
        this.timestamp = timestamp;
        this.executor = executor;
        this.compIdStr = compId;
        this.topoIdStr = topoIdStr;
        this.stream = stream;
        this.setValue(value);
    }

    public Metric(Metric o) {
        this.aggLevel = o.getAggLevel();
        this.topoIdStr = o.getTopoIdStr();
        this.timestamp = o.getTimeStamp();
        this.metricName = o.getMetricName();
        this.compIdStr = o.getCompName();
        this.executor = o.getExecutor();
        this.host = o.getHost();
        this.port = o.getPort();
        this.stream = o.getStream();

        this.count = o.getCount();
        this.value = o.value;
        this.sum = o.getSum();
        this.min = o.getMin();
        this.max = o.getMax();
    }

    public boolean equals(Object o) {

        if (o instanceof Metric == false)
            return false;

        Metric other = (Metric) o;

        return this == other ||
                (this.metricName.equals(other.getMetricName()) &&
                        this.topoIdStr.equals(other.getTopoIdStr()) &&
                        this.host.equals(other.getHost()) &&
                        this.port == other.getPort() &&
                        this.compIdStr.equals(other.getCompName()) &&
                        this.timestamp == other.getTimeStamp() &&
                        this.executor.equals(other.getExecutor()) &&
                        this.stream.equals(other.getStream()) &&
                        this.count == other.getCount() &&
                        this.value == other.getValue() &&
                        this.sum == other.getSum() &&
                        this.min == other.getMin() &&
                        this.max == other.getMax());

    }

    public void setValue(Double value) {
        this.count = 1L;
        this.min = value;
        this.max = value;
        this.sum = value;
        this.value = value;
    }

    public Double getValue() {
        //return this.value;
        // why?
        if (this.aggLevel == 0) {
            return this.value;
        } else {
            return this.sum;
        }
    }

    public void updateAverage(Double value) {
        this.count += 1;
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);
        this.sum += value;
        this.value = this.sum / this.count;
        LOG.debug("updating average {} {} {} {} {}", count, min, max, sum, value);
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getOwner() {
        return this.owner;
    }

    public void setAggLevel(Byte aggLevelInMins) {
        this.aggLevel = aggLevelInMins;
    }

    public Byte getAggLevel() {
        return this.aggLevel;
    }

    public void setCompName(String compName) {
        this.compIdStr = compName;
    }

    public String getCompName() {
        return this.compIdStr;
    }

    public void setTimeStamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getTimeStamp() {
        return this.timestamp;
    }

    public void setTopoIdStr(String topoIdStr) {
        this.topoIdStr = topoIdStr;
    }

    public String getTopoIdStr() {
        return this.topoIdStr;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public String getMetricName() {
        return this.metricName;
    }

    public void setExecutor(String executor) {
        this.executor = executor;
    }

    public String getExecutor() {
        return this.executor;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return this.host;
    }

    public void setPort(Long port) {
        this.port = port;
    }

    public Long getPort() {
        return this.port;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    public String getStream() {
        return this.stream;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getCount() {
        return this.count;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMin() {
        return this.min;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public Double getMax() {
        return this.max;
    }

    public void setSum(Double sum) {
        this.sum = sum;
    }

    public Double getSum() {
        return this.sum;
    }

    public String toString() {
        StringBuilder x      = new StringBuilder();
        Date          date   = new Date(this.timestamp);
        DateFormat    format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        x.append(format.format(date));
        x.append("|");
        x.append(this.topoIdStr);
        x.append("|");
        x.append(aggLevel);
        x.append("|");
        x.append(this.metricName);
        x.append("|");
        x.append(this.compIdStr);
        x.append("|");
        x.append(this.executor);
        x.append("|");
        x.append(this.host);
        x.append("|");
        x.append(this.port);
        x.append("|");
        x.append(this.stream);
        return String.format("%s -- count: %d -- value: %f -- min: %f -- max: %f -- sum: %f",
                x.toString(),
                this.count,
                this.value,
                this.min,
                this.max,
                this.sum);
    }
}
