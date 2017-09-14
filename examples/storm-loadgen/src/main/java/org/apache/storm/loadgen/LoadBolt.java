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

package org.apache.storm.loadgen;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bolt that simulates a real world bolt based off of statistics about it.
 */
public class LoadBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LoadBolt.class);
    private final List<OutputStream> outputStreamStats;
    private List<OutputStreamEngine> outputStreams;
    private final Map<GlobalStreamId, InputStream> inputStreams = new HashMap<>();
    private OutputCollector collector;
    private final ExecAndProcessLatencyEngine sleep;
    private int executorIndex;

    public LoadBolt(LoadCompConf conf) {
        this.outputStreamStats = Collections.unmodifiableList(new ArrayList<>(conf.streams));
        sleep = new ExecAndProcessLatencyEngine(conf.slp);
    }

    public void add(InputStream inputStream) {
        GlobalStreamId id = inputStream.gsid();
        inputStreams.put(id, inputStream);
    }

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        outputStreams = Collections.unmodifiableList(outputStreamStats.stream()
            .map((ss) -> new OutputStreamEngine(ss)).collect(Collectors.toList()));
        this.collector = collector;
        executorIndex = context.getThisTaskIndex();
        sleep.prepare();
    }

    private void emitTuples(Tuple input) {
        for (OutputStreamEngine se: outputStreams) {
            // we may output many tuples for a given input tuple
            while (se.shouldEmit() != null) {
                collector.emit(se.streamName, input, new Values(se.nextKey(), "SOME-BOLT-VALUE"));
            }
        }
    }

    @Override
    public void execute(final Tuple input) {
        long startTimeNs = System.nanoTime();
        InputStream in = inputStreams.get(input.getSourceGlobalStreamid());
        sleep.simulateProcessAndExecTime(executorIndex, startTimeNs, in, () -> {
            emitTuples(input);
            collector.ack(input);
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (OutputStream s: outputStreamStats) {
            declarer.declareStream(s.id, new Fields("key", "value"));
        }
    }
}
