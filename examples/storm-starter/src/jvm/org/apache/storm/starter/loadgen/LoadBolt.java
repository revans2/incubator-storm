/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.loadgen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.storm.metrics.hdrhistogram.HistogramMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * A bolt that simulates a real world bolt based off of statistics about it.
 */
public class LoadBolt extends BaseRichBolt {
    private final List<StreamStats> stats;
    private List<OutputStreamEngine> streams;
    private OutputCollector collector;

    public LoadBolt(StreamStats ... stats) {
        this(Arrays.asList(stats));
    }

    public LoadBolt(List<StreamStats> stats) {
        this.stats = Collections.unmodifiableList(new ArrayList<>(stats));
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        streams = Collections.unmodifiableList(stats.stream()
            .map((ss) -> new OutputStreamEngine(ss)).collect(Collectors.toList()));
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        for (OutputStreamEngine se: streams) {
            // we may output many tuples for a given input tuple
            while (se.shouldEmit() != null) {
                collector.emit(se.streamName, input, new Values(se.nextKey(), "SOME-BOLT-VALUE"));
            }
        }
        //TODO we need to simulate process latency and execute latency...
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (StreamStats s: stats) {
            declarer.declareStream(s.name, new Fields("key", "value"));
        }
    }
}
