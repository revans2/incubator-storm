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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerBackpressureThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerBackpressureThread.class);
    Object trigger;
    Object workerData;
    WorkerBackpressureCallback callback;

    public WorkerBackpressureThread(Object trigger, Object workerData, WorkerBackpressureCallback callback) {
        this.trigger = trigger;
        this.workerData = workerData;
        this.callback = callback;
        this.setName("WorkerBackpressureThread");
        this.setDaemon(true);
        this.setUncaughtExceptionHandler(new BackpressureUncaughtExceptionHandler());
    }

    static public void notifyBackpressureChecker(Object trigger) {
        try {
            synchronized (trigger) {
                trigger.notifyAll();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void run() {
        try {
            while (true) {
                synchronized(trigger) {
                    trigger.wait(100);
                }
                callback.onEvent(workerData); // check all executors and update zk backpressure throttle for the worker if needed
            }
        } catch (InterruptedException interEx) {
            LOG.info("WorkerBackpressureThread gets interrupted! Ignoring Exception: ", interEx);
        } catch (RuntimeException runEx) {
            LOG.error("Ignoring the failure in processing backpressure event: ", runEx);
        }
    }
}

class BackpressureUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureUncaughtExceptionHandler.class);
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            Utils.handleUncaughtException(e);
        } catch (Error error) {
            LOG.info("Received error in WorkerBackpressureThread.. terminating the worker...");
            Runtime.getRuntime().exit(1);
        }
    }
}
