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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.atomic.AtomicReference;

public class LoadAwareShuffleGrouping implements LoadAwareCustomStreamGrouping, Serializable {
    private static final int MAX = 100;

    public static class ListAndWeights {
        final List<Integer> list;
        int weight;

        ListAndWeights(int target) {
            list = Arrays.asList(target);
            weight = MAX;
        }
    }

    private Random random;
    private Map<Integer, ListAndWeights> orig;
    private final AtomicReference<ArrayList<List<Integer>>> choices = new AtomicReference<>();
    private long lastUpdate = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = ThreadLocalRandom.current();
        orig = new HashMap<>(targetTasks.size());
        for (Integer target: targetTasks) {
            orig.put(target, new ListAndWeights(target));
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        throw new RuntimeException("NOT IMPLEMENTED");
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values, LoadMapping load) {
        if ((lastUpdate + 1000) < System.currentTimeMillis()) {
            choices.set(updateChoices(load));
            lastUpdate = System.currentTimeMillis();
        }
        ArrayList<List<Integer>> c = choices.get();
        return c.get(random.nextInt(c.size()));
    }

    @VisibleForTesting
    ArrayList<List<Integer>> updateChoices(LoadMapping load) {
        ArrayList<List<Integer>> ret = new ArrayList<>(orig.size() * 100);
        double min = orig.keySet().stream().mapToDouble((key) -> load.get(key)).min().getAsDouble();
        for (Map.Entry<Integer, ListAndWeights> target: orig.entrySet()) {
            ListAndWeights val = target.getValue();
            double l = load.get(target.getKey());
            if (l <= min + (0.05)) {
                //We assume that within 5% of the minimum congestion is still fine.
                //Not congested we grow (but slowly)
                val.weight = Math.min(MAX, val.weight + 1);
            } else {
                //Congested we shrink by how much we are off from the others.
                val.weight = Math.max(0, val.weight - 10);
            }

            for (int i = 0; i < val.weight; i++) {
                ret.add(val.list);
            }
        }
        if (ret.size() == 0) {
            for (ListAndWeights law: orig.values()) {
                ret.add(law.list);
            }
        }
        return ret;
    }
}
