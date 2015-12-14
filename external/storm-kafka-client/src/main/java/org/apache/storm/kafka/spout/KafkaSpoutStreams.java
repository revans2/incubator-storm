/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.kafka.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents the output streams associated with each topic, and provides a public API to
 * declare output streams and emmit tuples, on the appropriate stream, for all the topics specified.
 */
public class KafkaSpoutStreams implements Serializable {
    private final Map<String, KafkaSpoutStream> topicToStream;

    private KafkaSpoutStreams(Builder builder) {
        this.topicToStream = builder.topicToStream;
    }

    /**
     * @param topic the topic for which to get output fields
     * @return the output fields declared
     */
    public Fields getOutputFields(String topic) {
        if (topicToStream.containsKey(topic)) {
            return topicToStream.get(topic).getOutputFields();
        }
        throw new IllegalStateException(this.getClass().getName() + " not configured for topic: " + topic);
    }

    /**
     * @param topic the topic to for which to get the stream id
     * @return the id of the stream to where the tuples are emitted
     */
    public String getStreamId(String topic) {
        if (topicToStream.containsKey(topic)) {
            return topicToStream.get(topic).getStreamId();
        }
        throw new IllegalStateException(this.getClass().getName() + " not configured for topic: " + topic);
    }

    /**
     * @return list of topics subscribed and emitting tuples to a stream as configured by {@link KafkaSpoutStream}
     */
    public List<String> getTopics() {
        return new ArrayList<>(topicToStream.keySet());
    }

    void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (KafkaSpoutStream stream : topicToStream.values()) {
            declarer.declareStream(stream.getStreamId(), stream.getOutputFields());
        }
    }

    void emit(SpoutOutputCollector collector, MessageId messageId) {
        collector.emit(getStreamId(messageId.topic()), messageId.getTuple(), messageId);
    }

    public static class Builder {
        private final Map<String, KafkaSpoutStream> topicToStream = new HashMap<>();;

        /**
         * Creates a {@link KafkaSpoutStream} with this particular stream for each topic specified.
         * All the topics will have the same stream id and output fields.
         */
        public Builder(Fields outputFields, String... topics) {
            this(outputFields, Utils.DEFAULT_STREAM_ID, topics);
        }

        /**
         * Creates a {@link KafkaSpoutStream} with this particular stream for each topic specified.
         * All the topics will have the same stream id and output fields.
         */
        public Builder (Fields outputFields, String streamId, String... topics) {
            for (String topic : topics) {
                topicToStream.put(topic, new KafkaSpoutStream(outputFields, streamId, topic));
            }
        }

        /**
         * Adds this stream to the state representing the streams associated with each topic
         */
        public Builder(KafkaSpoutStream stream) {
            topicToStream.put(stream.getTopic(), stream);
        }

        /**
         * Adds this stream to the state representing the streams associated with each topic
         */
        public Builder addStream(KafkaSpoutStream stream) {
            topicToStream.put(stream.getTopic(), stream);
            return this;
        }

        /**
         * Please refer to javadoc in {@link #Builder(Fields, String...)}
         */
        public Builder addStream(Fields outputFields, String... topics) {
            for (String topic : topics) {
                topicToStream.put(topic, new KafkaSpoutStream(outputFields, topic));
            }
            return this;
        }

        /**
         * Please refer to javadoc in {@link #Builder(Fields, String, String...)}
         */
        public Builder addStream(Fields outputFields, String streamId, String... topics) {
            for (String topic : topics) {
                topicToStream.put(topic, new KafkaSpoutStream(outputFields, streamId, topic));
            }
            return this;
        }

        public KafkaSpoutStreams build() {
            return new KafkaSpoutStreams(this);
        }
    }
}
