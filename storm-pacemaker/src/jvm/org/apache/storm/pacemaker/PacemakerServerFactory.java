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
import java.util.Map;
import java.util.HashSet;
import java.io.IOException;
import org.apache.storm.pacemaker.codec.ThriftNettyServerCodec;
import org.apache.storm.pacemaker.codec.ThriftNettyClientCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.Config;

import javax.security.auth.login.Configuration;
import backtype.storm.security.auth.AuthUtils;

public class PacemakerServerFactory {

    private static final Logger LOG = LoggerFactory
        .getLogger(PacemakerServerFactory.class);

    private static String makePayload(Map config) {

        String username = null;
        String password = null;
        try {
            Configuration login_config = AuthUtils.GetConfiguration(config);
            HashSet<String> desired = new HashSet<String>();
            desired.add("username");
            desired.add("password");
            Map<String, ?> results = AuthUtils.PullConfig(desired, login_config, "PacemakerConf");
            username = (String)results.get("username");
            password = (String)results.get("password");
        }
        catch (Exception e) {
            LOG.error("Failed to pull username/password out of jaas conf", e);
        }


        if(username == null || password == null) {
            LOG.error("Can't start pacemaker without SASL digest.");
            throw new RuntimeException("Can't start pacemaker without SASL digest.");
        }

        return username + ":" + password;
    }

    public static PacemakerServer makeServer(Map config, IServerMessageHandler handler) {

        int port = (int)config.get(Config.PACEMAKER_PORT);
        int maxWorkers = (int)config.get(Config.PACEMAKER_MAX_THREADS);
        String payload = makePayload(config);


        LOG.info("Making Pacemaker Server bound to port: " + Integer.toString(port));

        PacemakerServer server = new PacemakerServer(port, handler, "pacemaker-server", payload, maxWorkers);
        return server;
    }

    public static PacemakerClient makeClient(Map config) {

        String topo_name = (String)config.get(Config.TOPOLOGY_NAME);
        String host = (String)config.get(Config.PACEMAKER_HOST);
        int port = (int)config.get(Config.PACEMAKER_PORT);
        boolean shouldAuthenticate = false;

        if(topo_name == null || topo_name.equals("")) {
            topo_name = "none";
        }

        String payload = null;
        try {
            payload = makePayload(config);
        }
        catch (RuntimeException e) {
            LOG.info("No payload for: " + topo_name + ". Not going to authenticate.");
        }

        if(payload != null && !payload.equals("")) {
            shouldAuthenticate = true;
        }

        PacemakerClient pmc = new PacemakerClient(topo_name, payload, host, port, shouldAuthenticate);
        return pmc;
    }
}
