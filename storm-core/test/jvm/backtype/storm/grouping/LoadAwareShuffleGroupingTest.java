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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.WorkerTopologyContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class LoadAwareShuffleGroupingTest {
    @Test
    public void updateChoices() throws Exception {
        LoadAwareShuffleGrouping grouping = new LoadAwareShuffleGrouping();
        WorkerTopologyContext context = mock(WorkerTopologyContext.class);
        grouping.prepare(context, new GlobalStreamId("a", "default"), Arrays.asList(1, 2));

        Map<Integer, Double> localLoad = new HashMap<>();
        localLoad.put(1, 1.0);
        localLoad.put(2, 0.0);
        LoadMapping lm = new LoadMapping();
        lm.setLocal(localLoad);
        //First verify that if something has a high load it's distribution will drop over time
        for (int i = 9; i >= 0; i--) {
            Map<Integer, Integer> countByType = count(grouping.updateChoices(lm));
            assertEquals((Integer) 100, countByType.getOrDefault(2, 0));
            assertEquals((Integer)(i * 10), countByType.getOrDefault(1, 0));
        }

        //Now verify that when it is switched we can recover
        localLoad.put(1, 0.0);
        localLoad.put(2, 1.0);
        lm.setLocal(localLoad);
        int growingValue = 1;
        for (int i = 9; i >= 0; i--) {
            Map<Integer, Integer> countByType = count(grouping.updateChoices(lm));
            assertEquals("iteration " + growingValue, (Integer) growingValue , countByType.getOrDefault(1, 0));
            assertEquals("iteration " + growingValue, (Integer)(i * 10), countByType.getOrDefault(2, 0));

            growingValue++;
        }

        while (growingValue < 100) {
            Map<Integer, Integer> countByType = count(grouping.updateChoices(lm));
            assertEquals("iteration " + growingValue, (Integer) growingValue , countByType.getOrDefault(1, 0));
            assertEquals("iteration " + growingValue, (Integer) 0, countByType.getOrDefault(2, 0));

            growingValue++;
        }
    }

    private Map<Integer,Integer> count(ArrayList<List<Integer>> lists) {
        Map<Integer, Integer> ret = new HashMap<>();
        for (Map.Entry<Integer, List<List<Integer>>> entry:
            lists.stream().collect(Collectors.groupingBy((l) -> l.get(0))).entrySet()) {
            ret.put(entry.getKey(), entry.getValue().size());
        }
        return ret;
    }
}