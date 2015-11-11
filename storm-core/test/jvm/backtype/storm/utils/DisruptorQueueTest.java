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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Assert;
import org.junit.Test;
import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorQueueTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(DisruptorQueueTest.class);
    
    private final static int TIMEOUT = 1000; // MS
    private final static int PRODUCER_NUM = 4;

    @Test
    public void testFirstMessageFirst() throws InterruptedException {
      LOG.info("testFirstMessageFirst");
      for (int i = 0; i < 100; i++) {
        LOG.info("testFirstMessageFirst {}", i);
        DisruptorQueue queue = createQueue("firstMessageOrder", 16);

        queue.publish("FIRST");

        Runnable producer = new IncProducer(queue, i+100);

        final AtomicReference<Object> result = new AtomicReference<>();
        Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
            private boolean head = true;

            @Override
            public void onEvent(Object obj, long sequence, boolean endOfBatch)
                    throws Exception {
                if (head) {
                    head = false;
                    result.set(obj);
                }
            }
        });

        run(producer, consumer);
        Assert.assertEquals("We expect to receive first published message first, but received " + result.get(),
                "FIRST", result.get());
        LOG.info("testFirstMessageFirst {} DONE", i);
      }
    }
   
    @Test 
    public void testInOrder() throws InterruptedException {
        LOG.info("testInOrder");
        final AtomicBoolean allInOrder = new AtomicBoolean(true);

        DisruptorQueue queue = createQueue("consumerHang", 1024);
        Runnable producer = new IncProducer(queue, 1024*1024);
        Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
            long _expected = 0;
            @Override
            public void onEvent(Object obj, long sequence, boolean endOfBatch)
                    throws Exception {
                if (_expected != ((Number)obj).longValue()) {
                    allInOrder.set(false);
                    System.out.println("Expected "+_expected+" but got "+obj);
                }
                _expected++;
            }
        });

        run(producer, consumer, 1000, 1);
        Assert.assertTrue("Messages delivered out of order",
                allInOrder.get());
    }

    @Test 
    public void testInOrderBatch() throws InterruptedException {
        LOG.info("testInOrderBatch");
        final AtomicBoolean allInOrder = new AtomicBoolean(true);

        DisruptorQueue queue = createQueue("consumerHang", 10, 1024);
        Runnable producer = new IncProducer(queue, 1024*1024);
        Runnable consumer = new Consumer(queue, new EventHandler<Object>() {
            long _expected = 0;
            @Override
            public void onEvent(Object obj, long sequence, boolean endOfBatch)
                    throws Exception {
                if (_expected != ((Number)obj).longValue()) {
                    allInOrder.set(false);
                    System.out.println("Expected "+_expected+" but got "+obj);
                }
                _expected++;
            }
        });

        run(producer, consumer, 1000, 1);
        Assert.assertTrue("Messages delivered out of order",
                allInOrder.get());
    }


    private void run(Runnable producer, Runnable consumer)
            throws InterruptedException {
        run(producer, consumer, 10, PRODUCER_NUM);
    }

    private void run(Runnable producer, Runnable consumer, int sleepMs, int producerNum)
            throws InterruptedException {

        LOG.info("Starting {} producers and 1 consumer...", producerNum);
        Thread[] producerThreads = new Thread[producerNum];
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i] = new Thread(producer);
            producerThreads[i].start();
        }
        
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        Thread.sleep(sleepMs);
        for (int i = 0; i < producerNum; i++) {
            producerThreads[i].interrupt();
        }
        consumerThread.interrupt();
        
        for (int i = 0; i < producerNum; i++) {
            LOG.info("Joining producer {}",i);
            producerThreads[i].join(TIMEOUT);
            assertFalse("producer "+i+" is still alive", producerThreads[i].isAlive());
            LOG.info("DONE");
        }
        LOG.info("Joining consumer");
        consumerThread.join(TIMEOUT);
        assertFalse("consumer is still alive", consumerThread.isAlive());
        LOG.info("DONE");
    }

    private static class IncProducer implements Runnable {
        private DisruptorQueue queue;
        private long _max;

        IncProducer(DisruptorQueue queue, long max) {
            this.queue = queue;
            this._max = max;
        }

        @Override
        public void run() {
            for (long i = 0; i < _max && !(Thread.currentThread().isInterrupted()); i++) {
                queue.publish(i);
            }
        }
    }

    private static class Consumer implements Runnable {
        private EventHandler handler;
        private DisruptorQueue queue;

        Consumer(DisruptorQueue queue, EventHandler handler) {
            this.handler = handler;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while(true) {
                    queue.consumeBatchWhenAvailable(handler);
                }
            } catch(RuntimeException e) {
                //break
            }
        }
    }

    private static DisruptorQueue createQueue(String name, int queueSize) {
        return new DisruptorQueue(name, ProducerType.MULTI, queueSize, 0L, 1, 1L);
    }

    private static DisruptorQueue createQueue(String name, int batchSize, int queueSize) {
        return new DisruptorQueue(name, ProducerType.MULTI, queueSize, 0L, batchSize, 1L);
    }
}
