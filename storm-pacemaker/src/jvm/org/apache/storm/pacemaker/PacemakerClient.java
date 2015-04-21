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
package org.apache.storm.pacemaker;

import backtype.storm.messaging.netty.ISaslClient;
import backtype.storm.generated.Message;
import java.util.concurrent.LinkedBlockingQueue;
import org.jboss.netty.channel.Channel;

import com.twitter.finagle.Service;
import com.twitter.util.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PacemakerClient implements ISaslClient {

    private static class Task {
        public Message m;
        public Future f;
    }
    
    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClient.class);
    private LinkedBlockingQueue<Task> message_queue;

    private Service finagle_client;
    private String topo_name;
    private String secret;
    private boolean ready = false;
    private Object sendCond;
    
    public PacemakerClient(String topo_name, String secret) {
        this.topo_name = topo_name;
        this.secret = secret;
        message_queue = new LinkedBlockingQueue<Task>();
    }

    public void setClient(Service client) {
        finagle_client = client;
    }
    
    public void channelConnected(Channel channel) {
        LOG.info("Channel is connected.");
    }
    
    public synchronized void channelReady() {
        LOG.info("Got Channel Ready.");
        ready = true;
        this.notifyAll();
    }

    public String topologyName() {
        return topo_name;
    }

    public String secretKey() {
        return secret;
    }

    public synchronized Future send(Message m) {
        if(finagle_client == null) {
            throw new RuntimeException("Tried to send on PacemakerClient without setting the finagle client with setClient(...)");
        }
        
        if(!ready) {
            finagle_client.apply(null);
            LOG.info("Waiting for netty channel to be ready.");
            try {
                this.wait();
            } catch (java.lang.InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        LOG.info("Sending finagle message: " + m.toString());
        return finagle_client.apply(m);
    }

    public Future close() {
        return finagle_client.close();
    }
}
