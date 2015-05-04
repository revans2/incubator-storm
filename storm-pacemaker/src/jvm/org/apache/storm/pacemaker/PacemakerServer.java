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

import java.net.InetSocketAddress;
import java.lang.InterruptedException;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import backtype.storm.messaging.netty.ISaslServer;
import backtype.storm.generated.Message;
import backtype.storm.messaging.netty.NettyRenameThreadFactory;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PacemakerServer implements ISaslServer {

    private static final Logger LOG = LoggerFactory.getLogger(PacemakerServer.class);

    private final ServerBootstrap bootstrap;
    private int port;
    private IServerMessageHandler handler;
    private String secret;
    private String topo_name;
    private volatile ChannelGroup allChannels = new DefaultChannelGroup("storm-server");
    private ConcurrentSkipListSet<Channel> authenticated_channels = new ConcurrentSkipListSet<Channel>();
    
    
    public PacemakerServer(int port, IServerMessageHandler handler, String topo_name, String secret, int maxWorkers){
        this.port = port;
        this.handler = handler;
        this.topo_name = topo_name;
        this.secret = secret;
        
        ThreadFactory bossFactory = new NettyRenameThreadFactory("server-boss");
        ThreadFactory workerFactory = new NettyRenameThreadFactory("server-worker");
        
        NioServerSocketChannelFactory factory;
        if(maxWorkers > 0) {
            factory =
                new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                                                  Executors.newCachedThreadPool(workerFactory),
                                                  maxWorkers);
        }
        else {
            factory =
                new NioServerSocketChannelFactory(Executors.newCachedThreadPool(bossFactory),
                                                  Executors.newCachedThreadPool(workerFactory));
        }
        
        bootstrap = new ServerBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", 5242880);
        bootstrap.setOption("keepAlive", true);

        ChannelPipelineFactory pipelineFactory = new ThriftNettyServerCodec(this).pipelineFactory();
        bootstrap.setPipelineFactory(pipelineFactory);
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        allChannels.add(channel);
        LOG.info("Bound server to port: {}", Integer.toString(port));
    }

    /** Implementing IServer. **/
    public void channelConnected(Channel c) {
        allChannels.add(c);
    }
    
    public void received(Object mesg, String remote, Channel channel) throws InterruptedException {
        Message m = (Message)mesg;
        LOG.debug("received message. Passing to handler. {} : {} : {}",
                  handler.toString(), m.toString(), channel.toString());
        Message response = handler.handleMessage(m, authenticated_channels.contains(channel));
	LOG.debug("Got Response from handler: {}", response.toString());
        channel.write(response).await();
    }

    public void closeChannel(Channel c) {
        c.close().awaitUninterruptibly();
        allChannels.remove(c);
        authenticated_channels.remove(c);
    }

    public String name() {
        return topo_name;
    }

    public String secretKey() {
        return secret;
    }

    public void authenticated(Channel c) {
        LOG.debug("Pacemaker server authenticated channel: {}", c.toString());
        authenticated_channels.add(c);
    }
}
