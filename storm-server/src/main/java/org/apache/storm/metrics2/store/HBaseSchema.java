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


import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class HBaseSchema {

    public class MetricsTableInfo {

        private TableName tableName;
        private HColumnDescriptor descriptor;
        private byte[] columnFamily;
        private byte[] valueColumn;
        private byte[] sumColumn;
        private byte[] countColumn;
        private byte[] minColumn;
        private byte[] maxColumn;

        public MetricsTableInfo(String namespace,
                                String tableName,
                                String columnFamily,
                                String valueColumn,
                                String sumColumn,
                                String countColumn,
                                String minColumn,
                                String maxColumn) {
            this.tableName = TableName.valueOf(namespace, tableName);
            this.columnFamily = Bytes.toBytes(columnFamily);
            this.valueColumn = Bytes.toBytes(valueColumn);
            this.sumColumn = (sumColumn != null) ? Bytes.toBytes(sumColumn) : null;
            this.countColumn = (countColumn != null) ? Bytes.toBytes(countColumn) : null;
            this.minColumn = (minColumn != null) ? Bytes.toBytes(minColumn) : null;
            this.maxColumn = (maxColumn != null) ? Bytes.toBytes(maxColumn) : null;

            this.descriptor = new HColumnDescriptor(columnFamily);
            this.descriptor.setMaxVersions(HConstants.ALL_VERSIONS);
        }

        public TableName getTableName() {
            return tableName;
        }

        public HColumnDescriptor getDescriptor() {
            return descriptor;
        }

        public byte[] getColumnFamily() {
            return columnFamily;
        }

        public byte[] getValueColumn() {
            return valueColumn;
        }

        public byte[] getSumColumn() {
            return sumColumn;
        }

        public byte[] getCountColumn() {
            return countColumn;
        }

        public byte[] getMinColumn() {
            return minColumn;
        }

        public byte[] getMaxColumn() {
            return maxColumn;
        }
    }

    public class MetadataTableInfo {

        private TableName tableName;
        private HColumnDescriptor descriptor;
        private byte[] columnFamily;
        private byte[] column;
        private byte[] refcounter;

        public MetadataTableInfo(String namespace, String tableName, String columnFamily,
                                 String column, String refcounter) {
            this.tableName = TableName.valueOf(namespace, tableName);
            this.columnFamily = Bytes.toBytes(columnFamily);
            this.column = Bytes.toBytes(column);
            this.refcounter = Bytes.toBytes(refcounter);
            this.descriptor = new HColumnDescriptor(columnFamily);
            this.descriptor.setMaxVersions(HConstants.ALL_VERSIONS);
        }

        public TableName getTableName() {
            return tableName;
        }

        public HColumnDescriptor getDescriptor() {
            return descriptor;
        }

        public byte[] getColumnFamily() {
            return columnFamily;
        }

        public byte[] getColumn() {
            return column;
        }

        public byte[] getRefcounter() {
            return refcounter;
        }
    }

    private static final String SCHEMA_KEY = "storm.metrics2.store.HBaseStore.hbase.schema";

    private HBaseSchemaType schemaType;

    public MetricsTableInfo metricsTableInfo;
    public MetadataTableInfo[] metadataTableInfos;

    public HBaseSchema(Map conf) throws MetricException {

        validateSchema(conf);

        HashMap<String, Object> schemaMap = (HashMap<String, Object>) conf.get(SCHEMA_KEY);
        HashMap<String, Object> metricsMap = (HashMap<String, Object>) schemaMap.get("metrics");
        HashMap<String, Object> metadataMap = (HashMap<String, Object>) schemaMap.get("metadata");

        String schemaTypeStr = (String) schemaMap.get("type");
        this.schemaType = HBaseSchemaType.fromKey(schemaTypeStr);

        createMetricsDescriptor(metricsMap);

        this.metadataTableInfos = new MetadataTableInfo[HBaseMetadataIndex.count()];
        metadataMap.forEach((metadataType, map) -> {
            HashMap<String, String> tableMap = (HashMap<String, String>) map;
            createMetadataDescriptor(metadataType, tableMap);
        });

    }

    public HBaseSchemaType getSchemaType() {
        return this.schemaType;
    }

    public HashMap<TableName, ArrayList<HColumnDescriptor>> getTableMap() {

        HashMap<TableName, ArrayList<HColumnDescriptor>> tableMap = new HashMap<>();

        tableMap.put(metricsTableInfo.tableName, new ArrayList<>());
        for (MetadataTableInfo metadataTableInfo : metadataTableInfos) {
            tableMap.put(metadataTableInfo.tableName, new ArrayList<>());
        }

        tableMap.get(metricsTableInfo.tableName).add(metricsTableInfo.descriptor);
        for (MetadataTableInfo metadataTableInfo : metadataTableInfos) {
            tableMap.get(metadataTableInfo.tableName).add(metadataTableInfo.descriptor);
        }

        return tableMap;
    }

    private void createMetricsDescriptor(HashMap<String, Object> metricsMap) {

        String namespace = (String) metricsMap.get("namespace");
        String name = (String) metricsMap.get("name");
        String columnFamily = (String) metricsMap.get("cf");

        if (schemaType == HBaseSchemaType.COMPACT) {

            String valueColumn = (String) metricsMap.get("column");

            this.metricsTableInfo = new MetricsTableInfo(namespace, name, columnFamily, valueColumn, null,
                    null, null, null);

        } else if (schemaType == HBaseSchemaType.EXPANDED) {

            HashMap<String, String> columnMap = (HashMap<String, String>) metricsMap.get("columns");
            String valueColumn = columnMap.get("value");
            String sumColumn = columnMap.get("sum");
            String countColumn = columnMap.get("count");
            String minColumn = columnMap.get("min");
            String maxColumn = columnMap.get("max");

            this.metricsTableInfo = new MetricsTableInfo(namespace, name, columnFamily, valueColumn, sumColumn,
                    countColumn, minColumn, maxColumn);
        }

    }

    private void createMetadataDescriptor(String metadataType, HashMap<String, String> tableMap) {

        String namespace = tableMap.get("namespace");
        String name = tableMap.get("name");
        String columnFamily = tableMap.get("cf");
        String column = tableMap.get("column");
        String refcounter = tableMap.get("refcounter");

        MetadataTableInfo info = new MetadataTableInfo(namespace, name, columnFamily, column, refcounter);
        int index = HBaseMetadataIndex.indexFromMapping(metadataType);
        metadataTableInfos[index] = info;

    }

    private void validateSchema(Map conf) throws MetricException {

        HashMap<String, Object> schemaMap = (HashMap<String, Object>) conf.get(SCHEMA_KEY);

        if (schemaMap == null)
            throw new MetricException("No schema specified");


        String schemaTypeStr = (String) schemaMap.get("type");
        HashMap<String, Object> metricsMap = (HashMap<String, Object>) schemaMap.get("metrics");
        HashMap<String, Object> metadataMap = (HashMap<String, Object>) schemaMap.get("metadata");

        if (schemaTypeStr == null)
            throw new MetricException("No schema type specified");

        if (metricsMap == null)
            throw new MetricException("No metrics schema specified");

        if (metadataMap == null)
            throw new MetricException("No metadata schema specified");

        HBaseSchemaType schemaType = HBaseSchemaType.fromKey(schemaTypeStr);

        switch (schemaType) {
            case COMPACT:
                validateMetricsSchemaCompact(metricsMap);
                break;
            case EXPANDED:
                validateMetricsSchemaExpanded(metricsMap);
                break;
            default:
                throw new MetricException("Unknown schema type specfied");
        }

        validateMetadataSchema(metadataMap);

    }

    private void validateMetricsSchemaCompact(HashMap<String, Object> metricsMap) throws MetricException {

        if (!metricsMap.containsKey("name") ||
                !metricsMap.containsKey("cf") ||
                !metricsMap.containsKey("column")) {
            throw new MetricException("Invalid metrics map");
        }

    }

    private void validateMetricsSchemaExpanded(HashMap<String, Object> metricsMap) throws MetricException {

        if (!metricsMap.containsKey("name") ||
                !metricsMap.containsKey("cf") ||
                !metricsMap.containsKey("columns")) {
            throw new MetricException("Invalid metrics map");
        }

        HashMap<String, String> metricsColumns = (HashMap<String, String>) metricsMap.get("columns");
        if (!metricsColumns.containsKey("value") ||
                !metricsColumns.containsKey("count") ||
                !metricsColumns.containsKey("sum") ||
                !metricsColumns.containsKey("min") ||
                !metricsColumns.containsKey("max")) {
            throw new MetricException("Invalid metrics columns map");
        }
    }

    private void validateMetadataSchema(HashMap<String, Object> metadataMap) throws MetricException {

        // for metadata that share common tables, refcounter should be unique

        List<String> metadataNames = Arrays.asList("topoMap",
                "streamMap",
                "hostMap",
                "compMap",
                "metricMap",
                "executorMap");

        for (String metaName : metadataNames) {
            HashMap<String, String> metaMap = (HashMap<String, String>) metadataMap.get(metaName);

            if (!metaMap.containsKey("name") ||
                    !metaMap.containsKey("cf") ||
                    !metaMap.containsKey("column") ||
                    !metaMap.containsKey("refcounter")) {
                throw new MetricException("Invalid metadata map for " + metaName);
            }
        }

    }

}
