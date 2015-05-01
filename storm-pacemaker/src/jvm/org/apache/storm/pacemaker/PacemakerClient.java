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

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import backtype.storm.messaging.netty.ISaslClient;
import backtype.storm.generated.Message;
import backtype.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.pacemaker.codec.ThriftNettyClientCodec;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.messaging.netty.Client;
import backtype.storm.messaging.netty.Context;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Future;

public class PacemakerClient implements ISaslClient {

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerClient.class);
    private LinkedBlockingQueue<Message> message_queue;

    private String topo_name;
    private String secret;
    private boolean ready = false;
    private final ClientBootstrap bootstrap;
    private AtomicReference<Channel> channelRef;
    private AtomicBoolean closing;
    private InetSocketAddress remote_addr;
    private int maxPending = 100;
    private Message outstanding[];
    private AtomicInteger nextMID;

    public PacemakerClient(String topo_name, String secret, String host, int port, boolean shouldAuthenticate) {
        this.topo_name = topo_name;
        this.secret = secret;
        message_queue = new LinkedBlockingQueue<Message>();
        closing = new AtomicBoolean(false);
        channelRef = new AtomicReference<Channel>(null);
        outstanding = new Message[maxPending];
        nextMID = new AtomicInteger(0);

        ThreadFactory bossFactory = new NettyRenameThreadFactory("client-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("client-worker");
        NioClientSocketChannelFactory factory =
            new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                                              Executors.newCachedThreadPool(workerFactory));
        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", 5242880);
        bootstrap.setOption("keepAlive", true);

        remote_addr = new InetSocketAddress(host, port);
        ChannelPipelineFactory pipelineFactory = new ThriftNettyClientCodec(this, shouldAuthenticate).pipelineFactory();
        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.connect(remote_addr);
    }

    public void channelConnected(Channel channel) {
        LOG.debug("Channel is connected: " + channel.toString());
        channelRef.set(channel);
    }

    public synchronized void channelReady() {
        LOG.debug("Got Channel Ready.");
        ready = true;
        this.notifyAll();
    }

    public String topologyName() {
        return topo_name;
    }

    public String secretKey() {
        return secret;
    }

    public synchronized Message send(Message m) {

        if(!ready) {
            LOG.debug("Waiting for netty channel to be ready.");
            try {
                this.wait();
            } catch (java.lang.InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        LOG.info("Getting Next!");
        // Standard CAS loop
        int next;
        int expect;
        do {
            expect = nextMID.get();
            next = (expect + 1) % maxPending;
        } while(!nextMID.compareAndSet(expect, next));
        m.set_message_id(next);

        LOG.info("Sending finagle message: " + m.toString());
        try {
            channelRef.get().write(m).await();
            // Wait for other task to finish.
            if(outstanding[next] != null) {
                synchronized(outstanding[next]) {
                    outstanding[next].wait();
                }
            }

            outstanding[next] = m;
            synchronized (m) {
                m.wait();
            }
            Message ret = outstanding[next];
            outstanding[next] = null;
            return ret;
        }
        catch (InterruptedException e) {
            LOG.error("PacemakerClient send interrupted: ", e);
            throw new RuntimeException(e);
        }
    }

    public void gotMessage(Message m) {
        int message_id = m.get_message_id();
        if(message_id >=0 && message_id < maxPending) {
            LOG.debug("Pacemaker Client got message: " + m.toString());
            Message request = outstanding[message_id];
            outstanding[message_id] = m;
            synchronized(request) {
                request.notifyAll();
            }
        }
        else {
            LOG.error("Got Message with bad id: " + m.toString());
        }
    }

    public void reconnect() {
        close_channel();
        if(closing.get()) return;
        bootstrap.connect(remote_addr);
    }

    synchronized void close_channel() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed",remote_addr);
            channelRef.set(null);
        }
    }

    public void close() {
        close_channel();
    }
}
