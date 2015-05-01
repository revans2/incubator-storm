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
package org.apache.storm.pacemaker.codec;

import java.io.IOException;

import com.twitter.finagle.AbstractCodec;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import backtype.storm.messaging.netty.SaslStormServerHandler;
import backtype.storm.messaging.netty.StormServerHandler;
import backtype.storm.messaging.netty.SaslStormServerAuthorizeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.netty.IServer;
import backtype.storm.messaging.netty.ISaslServer;

public class ThriftNettyServerCodec extends AbstractCodec{

    private IServer server;

    private static final Logger LOG = LoggerFactory
        .getLogger(ThriftNettyServerCodec.class);

    public ThriftNettyServerCodec(IServer server) {
        this.server = server;
    }

    public ChannelPipelineFactory pipelineFactory() {
        return new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {

                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("encoder", new ThriftEncoder());
                pipeline.addLast("decoder", new ThriftDecoder());
                try {
                    LOG.debug("Adding SaslStormServerHandler to pacemaker server pipeline.");
                    pipeline.addLast("sasl-handler", new SaslStormServerHandler((ISaslServer)server));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }

                pipeline.addLast("handler", new StormServerHandler(server));
                return pipeline;
            }
        };
    }
}
