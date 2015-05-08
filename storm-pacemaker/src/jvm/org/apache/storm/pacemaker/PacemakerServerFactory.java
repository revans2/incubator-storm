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

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String CONFIG_SECTION = "PacemakerConf";
    
    private static final Logger LOG = LoggerFactory
        .getLogger(PacemakerServerFactory.class);

    private static String makePayload(Map config) {
        String username = null;
        String password = null;
        try {
            Configuration login_config = AuthUtils.GetConfiguration(config);
            Map<String, ?> results = AuthUtils.PullConfig(login_config, CONFIG_SECTION);
            username = (String)results.get(USERNAME);
            password = (String)results.get(PASSWORD);
        }
        catch (Exception e) {
            LOG.error("Failed to pull username/password out of jaas conf", e);
        }

        if(username == null || password == null) {
            throw new RuntimeException("No username or password from jaas conf.");
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
