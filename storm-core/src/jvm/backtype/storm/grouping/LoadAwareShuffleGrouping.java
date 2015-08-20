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
package backtype.storm.grouping;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private Random random;
    private Map<Integer, List<Integer>> orig;
    private ArrayList<List<Integer>> choices;
    private long lastUpdate = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = ThreadLocalRandom.current();
        orig = new HashMap<Integer, List<Integer>>(targetTasks.size());
        for (Integer target: targetTasks) {
            orig.put(target, Arrays.asList(target));
        }
        choices = new ArrayList<List<Integer>>(targetTasks.size() * 100);
    }
    
    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        throw new RuntimeException("NOT IMPLEMENTED");
    }

    @Override
    public synchronized List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
        if ((lastUpdate + 1000) < System.currentTimeMillis()) {
            choices.clear();
            for (Map.Entry<Integer, List<Integer>> target: orig.entrySet()) {
                int val = (int)(101 - (load.get(target.getKey()) * 100));
                for (int i = 0; i < val; i++) {
                    choices.add(target.getValue());
                }
            }
            lastUpdate = System.currentTimeMillis();
        }
        return choices.get(random.nextInt(choices.size()));
    }
}
