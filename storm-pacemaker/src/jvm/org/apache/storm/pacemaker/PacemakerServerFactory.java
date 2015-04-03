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
import org.apache.storm.pacemaker.codec.ThriftNettyCodec;

public class PacemakerServerFactory {

    public static Server makeServer(String name, int port, Service service) {

        return ServerBuilder.safeBuild(
            service,
            ServerBuilder.get()
            .name(name)
            .codec(new ThriftNettyCodec())
            .bindTo(new InetSocketAddress(port)));
    }

    public static Service makeClient(String host) {

        Service client =  ClientBuilder.safeBuild(
            ClientBuilder.get()
            .codec(new ThriftNettyCodec())
            .hosts(host)
            .hostConnectionLimit(1)
            .retryPolicy(new PacemakerRetryPolicy())
            );
        
        return client;
    }
}
