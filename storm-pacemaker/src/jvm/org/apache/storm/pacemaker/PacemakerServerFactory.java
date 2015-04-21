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

import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.Service;
import java.net.InetSocketAddress;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;
import org.apache.storm.pacemaker.codec.ThriftNettyClientCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacemakerServerFactory {

    private static final Logger LOG = LoggerFactory
        .getLogger(PacemakerServerFactory.class);
    
    public static Server makeServer(String name, int port, Service service) {

        LOG.info("Making Pacemaker Server bound to port: " + Integer.toString(port));
        return ServerBuilder.safeBuild(
            service,
            ServerBuilder.get()
            .name(name)
            .codec(new ThriftNettyServerCodec())
            .bindTo(new InetSocketAddress(port)));
    }

    public static PacemakerClient makeClient(String host) {

        PacemakerClient pmc = new PacemakerClient("whatever", "knusbaum:asdf1234");
        
        ThriftNettyClientCodec codec = new ThriftNettyClientCodec(pmc);
        
        LOG.info("Making Pacemaker Client.");
        Service client =  ClientBuilder.safeBuild(
            ClientBuilder.get()
            .codec(codec)
            .hosts(host)
            .hostConnectionLimit(1)
            .retryPolicy(new PacemakerRetryPolicy()));

        pmc.setClient(client);
        
        return pmc;
    }
}
