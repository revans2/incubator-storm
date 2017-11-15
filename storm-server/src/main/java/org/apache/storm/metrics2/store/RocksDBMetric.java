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

public class RocksDBMetric extends Metric {
    byte[] _key;
    Integer _topoId;
    Integer _metricId;
    Integer _compId;
    Integer _execId;
    Integer _hostId;
    Integer _streamId;

    public RocksDBMetric() {
    }

    public RocksDBMetric(byte[] key){
        //TODO: anyone use this?
        _key = key;
    }

    public void setKey(byte[] key) { _key = key; }
    public byte[] getKey() { return _key; }

    public void setTopoId(Integer topoId){ _topoId = topoId; }
    public Integer getTopoId(){ return _topoId; }

    public void setMetricId(Integer metricId){ _metricId = metricId; }
    public Integer getMetricId(){ return _metricId; }

    public void setCompId(Integer compId){ _compId = compId; }
    public Integer getCompId(){ return _compId; }

    public void setExecId(Integer execId){ _execId = execId; }
    public Integer getExecId(){ return _execId; }

    public void setHostId(Integer hostId){ _hostId = hostId; }
    public Integer getHostId() { return _hostId; }

    public void setStreamId(Integer streamId){ _streamId = streamId; }
    public Integer getStreamId(){ return _streamId; }
}
