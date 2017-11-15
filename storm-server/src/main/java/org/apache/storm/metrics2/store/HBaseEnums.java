/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;

/**
 * @see HBaseSerializer
 */
enum HBaseSchemaType {
    COMPACT("compact", "org.apache.storm.metrics2.store.HBaseSerializerCompact"),
    EXPANDED("expanded", "org.apache.storm.metrics2.store.HBaseSerializerExpanded");

    private String configKey;
    private String className;

    HBaseSchemaType(String configKey, String className) {
        this.configKey = configKey;
        this.className = className;
    }

    public String getClassName() {
        return this.className;
    }

    public static HBaseSchemaType fromKey(String key) {
        for (HBaseSchemaType type : HBaseSchemaType.values()) {
            if (type.configKey.equals(key))
                return type;
        }
        throw new IllegalArgumentException("Invalid schema type");
    }

}


enum HBaseMetadataIndex {

    TOPOLOGY(0, "topoMap"),
    STREAM(1, "streamMap"),
    HOST(2, "hostMap"),
    COMP(3, "compMap"),
    METRICNAME(4, "metricMap"),
    EXECUTOR(5, "executorMap");

    private int index;
    private String schemaMapping;

    HBaseMetadataIndex(int index, String schemaMapping) {
        this.index = index;
        this.schemaMapping = schemaMapping;
    }

    public int getIndex() {
        return index;
    }

    public String getSchemaMapping() {
        return schemaMapping;
    }

    public static int indexFromMapping(String mapping) {
        for (HBaseMetadataIndex item : HBaseMetadataIndex.values()) {
            if (item.schemaMapping.equals(mapping))
                return item.index;
        }
        throw new IllegalArgumentException("Invalid schema type");
    }

    public static int count() {
        return HBaseMetadataIndex.values().length;
    }
}