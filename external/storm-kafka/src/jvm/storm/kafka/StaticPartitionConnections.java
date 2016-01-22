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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

public class StaticPartitionConnections {

    public static final Logger LOG = LoggerFactory.getLogger(StaticPartitionConnections.class);

    Map<Integer, SimpleConsumer> _kafka = new HashMap<Integer, SimpleConsumer>();
    KafkaConfig _config;
    StaticHosts hosts;
    Constructor<SimpleConsumer> _simpleConsumerConstr;
    boolean useSecurityParamSimpleConsumer = false;

    public StaticPartitionConnections(KafkaConfig conf) {
        _config = conf;
        if (!(conf.hosts instanceof StaticHosts)) {
            throw new RuntimeException("Must configure with static hosts");
        }
        this.hosts = (StaticHosts) conf.hosts;
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        try {
            Class c = loader.loadClass("kafka.javaapi.consumer.SimpleConsumer");

            try {
                // This version of the constructor exists in our early Security version in 
                // 0.8.x and is here to keep backwards compatibility. If this constructor 
                // exists use it, otherwise fallback.
                _simpleConsumerConstr = c.getDeclaredConstructor(String.class, int.class, int.class,
                    int.class, String.class,
                    org.apache.kafka.common.protocol.SecurityProtocol.class);
                useSecurityParamSimpleConsumer = true;
            } catch (NoSuchMethodException e) {
                try {
                    // This is the constructor that exists in the open source kafka 0.8 
                    // and 0.9 versions
                     _simpleConsumerConstr = c.getDeclaredConstructor(String.class, int.class, int.class,
                         int.class, String.class);
                     useSecurityParamSimpleConsumer = false;
                } catch (NoSuchMethodException ne) {
                    LOG.error("Error finding constructor for kafka.javaapi.consumer.SimpleConsumer");
                }
            }
        } catch (ClassNotFoundException cne) {
            LOG.error("Error finding class kafka.javaapi.consumer.SimpleConsumer");
        }

    }

    public SimpleConsumer getConsumer(int partition) {
        if (!_kafka.containsKey(partition)) {
            Broker hp = hosts.getPartitionInformation().getBrokerFor(partition);
           SimpleConsumer sconsumer;
            try {
                if (useSecurityParamSimpleConsumer) {
                    sconsumer = _simpleConsumerConstr.newInstance(hp.host, hp.port,
                        _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId,
                        _config.securityProtocol);
                } else {
                    sconsumer = _simpleConsumerConstr.newInstance(hp.host, hp.port,
                        _config.socketTimeoutMs, _config.bufferSizeBytes, _config.clientId);
                }
                _kafka.put(partition, sconsumer);
            } catch (Exception ie) {
                LOG.error("Error creating new instance of kafka.javaapi.consumer.SimpleConsumer");
            }
        }
        return _kafka.get(partition);
    }

    public void close() {
        for (SimpleConsumer consumer : _kafka.values()) {
            consumer.close();
        }
    }
}
