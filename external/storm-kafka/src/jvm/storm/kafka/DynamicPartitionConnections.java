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
package storm.kafka;

import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.trident.IBrokerReader;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class DynamicPartitionConnections {

    public static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionConnections.class);

    static class ConnectionInfo {
        SimpleConsumer consumer;
        Set<Integer> partitions = new HashSet();

        public ConnectionInfo(SimpleConsumer consumer) {
            this.consumer = consumer;
        }
    }

    Map<Broker, ConnectionInfo> _connections = new HashMap();
    KafkaConfig _config;
    IBrokerReader _reader;
    Constructor<SimpleConsumer> _simpleConsumerConstr;
    boolean useSecurityParamSimpleConsumer = false;

    public DynamicPartitionConnections(KafkaConfig config, IBrokerReader brokerReader) {
        _config = config;
        _reader = brokerReader;
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        try {
            Class c = loader.loadClass("kafka.javaapi.consumer.SimpleConsumer");

            try {
                // This version of the constructor exists in our early Security version in 
                // 0.8.x and is here to keep backwards compatibility. If this constructor 
                // exists use it, otherwise fallback.
                LOG.debug("Try using SimpleConsumer api with security");
                _simpleConsumerConstr = c.getDeclaredConstructor(String.class, int.class, int.class, 
                    int.class, String.class, String.class);
                useSecurityParamSimpleConsumer = true;
            } catch (NoSuchMethodException e) {
                try {
                    // This is the constructor that exists in the open source kafka 0.8 
                    // and 0.9 versions
                    LOG.debug("Fall back to using open source SimpleConsumer api");
                     _simpleConsumerConstr = c.getDeclaredConstructor(String.class, int.class, 
                         int.class, int.class, String.class);
                     useSecurityParamSimpleConsumer = false;
                } catch (NoSuchMethodException ne) {
                    LOG.error("Error finding constructor for kafka.javaapi.consumer.SimpleConsumer");
                }
            }
        } catch (ClassNotFoundException cne) {
            LOG.error("Error finding class kafka.javaapi.consumer.SimpleConsumer");
        }
    }

    public SimpleConsumer register(Partition partition) {
        Broker broker = _reader.getCurrentBrokers().getBrokerFor(partition.partition);
        return register(broker, partition.partition);
    }

    public SimpleConsumer register(Broker host, int partition) {
        if (!_connections.containsKey(host)) {
            SimpleConsumer sconsumer; 
            try {
                if (useSecurityParamSimpleConsumer) {
                    sconsumer = _simpleConsumerConstr.newInstance(host.host, host.port, 
                        _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId, 
                        _config.securityProtocol);
                } else {
                    sconsumer = _simpleConsumerConstr.newInstance(host.host, host.port, 
                        _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId);
                }
                _connections.put(host, new ConnectionInfo(sconsumer));
            } catch (Exception ie) {
                LOG.error("Error creating new instance of kafka.javaapi.consumer.SimpleConsumer");
            }
        }
        ConnectionInfo info = _connections.get(host);
        info.partitions.add(partition);
        return info.consumer;
    }

    public SimpleConsumer getConnection(Partition partition) {
        ConnectionInfo info = _connections.get(partition.host);
        if (info != null) {
            return info.consumer;
        }
        return null;
    }

    public void unregister(Broker port, int partition) {
        ConnectionInfo info = _connections.get(port);
        info.partitions.remove(partition);
        if (info.partitions.isEmpty()) {
            info.consumer.close();
            _connections.remove(port);
        }
    }

    public void unregister(Partition partition) {
        unregister(partition.host, partition.partition);
    }

    public void clear() {
        for (ConnectionInfo info : _connections.values()) {
            info.consumer.close();
        }
        _connections.clear();
    }
}
