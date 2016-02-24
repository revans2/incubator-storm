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

package backtype.storm.utils;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerBackpressureThreadTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerBackpressureThreadTest.class);

    @Test
    public void testNormalEvent() throws Exception {
        Object trigger = new Object();
        AtomicLong workerData = new AtomicLong(0);
        WorkerBackpressureCallback callback = new WorkerBackpressureCallback() {
            @Override
            public void onEvent(Object obj) {
                ((AtomicLong) obj).getAndDecrement();
            }
        };
        WorkerBackpressureThread workerBackpressureThread = new WorkerBackpressureThread(trigger, workerData, callback);
        workerBackpressureThread.start();
        Thread.sleep(100);
        WorkerBackpressureThread.notifyBackpressureChecker(trigger);
        Thread.sleep(100);
        Assert.assertNotEquals("Check the calling times of backpressure events, should not be 0. ",
                workerData.get(), 0);
    }

    @Test
    public void testThrowRuntimeExceptionEvent() throws Exception {
        Object trigger = new Object();
        Object workerData = new Object();
        WorkerBackpressureCallback callback = new WorkerBackpressureCallback() {
            @Override
            public void onEvent(Object obj) {
                throw new RuntimeException();
            }
        };
        WorkerBackpressureThread workerBackpressureThread = new WorkerBackpressureThread(trigger, workerData, callback);
        workerBackpressureThread.start();
        Thread.sleep(100);
        WorkerBackpressureThread.notifyBackpressureChecker(trigger);
        Thread.sleep(100);
        Assert.assertFalse("Check the aliveness of workerBackpressureThread after RuntimeException. ",
                workerBackpressureThread.isAlive());
    }
}