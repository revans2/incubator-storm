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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;

public class ShuffleGrouping implements CustomStreamGrouping, Serializable {
    private Random random;
    private ArrayList<List<Integer>> choices;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        random = new Random();
        choices = new ArrayList<List<Integer>>(targetTasks.size());
        for (Integer i: targetTasks) {
            choices.add(Arrays.asList(i));
        }
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        return choices.get(random.nextInt(choices.size()));
    }
}
