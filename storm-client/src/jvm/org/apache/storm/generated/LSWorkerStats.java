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
/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class LSWorkerStats implements org.apache.thrift.TBase<LSWorkerStats, LSWorkerStats._Fields>, java.io.Serializable, Cloneable, Comparable<LSWorkerStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LSWorkerStats");

  private static final org.apache.thrift.protocol.TField TIME_STAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("time_stamp", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField METRICS_FIELD_DESC = new org.apache.thrift.protocol.TField("metrics", org.apache.thrift.protocol.TType.MAP, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LSWorkerStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LSWorkerStatsTupleSchemeFactory());
  }

  private long time_stamp; // optional
  private Map<String,Double> metrics; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TIME_STAMP((short)1, "time_stamp"),
    METRICS((short)2, "metrics");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TIME_STAMP
          return TIME_STAMP;
        case 2: // METRICS
          return METRICS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __TIME_STAMP_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.TIME_STAMP,_Fields.METRICS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TIME_STAMP, new org.apache.thrift.meta_data.FieldMetaData("time_stamp", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.METRICS, new org.apache.thrift.meta_data.FieldMetaData("metrics", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LSWorkerStats.class, metaDataMap);
  }

  public LSWorkerStats() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LSWorkerStats(LSWorkerStats other) {
    __isset_bitfield = other.__isset_bitfield;
    this.time_stamp = other.time_stamp;
    if (other.is_set_metrics()) {
      Map<String,Double> __this__metrics = new HashMap<String,Double>(other.metrics);
      this.metrics = __this__metrics;
    }
  }

  public LSWorkerStats deepCopy() {
    return new LSWorkerStats(this);
  }

  @Override
  public void clear() {
    set_time_stamp_isSet(false);
    this.time_stamp = 0;
    this.metrics = null;
  }

  public long get_time_stamp() {
    return this.time_stamp;
  }

  public void set_time_stamp(long time_stamp) {
    this.time_stamp = time_stamp;
    set_time_stamp_isSet(true);
  }

  public void unset_time_stamp() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TIME_STAMP_ISSET_ID);
  }

  /** Returns true if field time_stamp is set (has been assigned a value) and false otherwise */
  public boolean is_set_time_stamp() {
    return EncodingUtils.testBit(__isset_bitfield, __TIME_STAMP_ISSET_ID);
  }

  public void set_time_stamp_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TIME_STAMP_ISSET_ID, value);
  }

  public int get_metrics_size() {
    return (this.metrics == null) ? 0 : this.metrics.size();
  }

  public void put_to_metrics(String key, double val) {
    if (this.metrics == null) {
      this.metrics = new HashMap<String,Double>();
    }
    this.metrics.put(key, val);
  }

  public Map<String,Double> get_metrics() {
    return this.metrics;
  }

  public void set_metrics(Map<String,Double> metrics) {
    this.metrics = metrics;
  }

  public void unset_metrics() {
    this.metrics = null;
  }

  /** Returns true if field metrics is set (has been assigned a value) and false otherwise */
  public boolean is_set_metrics() {
    return this.metrics != null;
  }

  public void set_metrics_isSet(boolean value) {
    if (!value) {
      this.metrics = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TIME_STAMP:
      if (value == null) {
        unset_time_stamp();
      } else {
        set_time_stamp((Long)value);
      }
      break;

    case METRICS:
      if (value == null) {
        unset_metrics();
      } else {
        set_metrics((Map<String,Double>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TIME_STAMP:
      return get_time_stamp();

    case METRICS:
      return get_metrics();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TIME_STAMP:
      return is_set_time_stamp();
    case METRICS:
      return is_set_metrics();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LSWorkerStats)
      return this.equals((LSWorkerStats)that);
    return false;
  }

  public boolean equals(LSWorkerStats that) {
    if (that == null)
      return false;

    boolean this_present_time_stamp = true && this.is_set_time_stamp();
    boolean that_present_time_stamp = true && that.is_set_time_stamp();
    if (this_present_time_stamp || that_present_time_stamp) {
      if (!(this_present_time_stamp && that_present_time_stamp))
        return false;
      if (this.time_stamp != that.time_stamp)
        return false;
    }

    boolean this_present_metrics = true && this.is_set_metrics();
    boolean that_present_metrics = true && that.is_set_metrics();
    if (this_present_metrics || that_present_metrics) {
      if (!(this_present_metrics && that_present_metrics))
        return false;
      if (!this.metrics.equals(that.metrics))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_time_stamp = true && (is_set_time_stamp());
    list.add(present_time_stamp);
    if (present_time_stamp)
      list.add(time_stamp);

    boolean present_metrics = true && (is_set_metrics());
    list.add(present_metrics);
    if (present_metrics)
      list.add(metrics);

    return list.hashCode();
  }

  @Override
  public int compareTo(LSWorkerStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_time_stamp()).compareTo(other.is_set_time_stamp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_time_stamp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.time_stamp, other.time_stamp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_metrics()).compareTo(other.is_set_metrics());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_metrics()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.metrics, other.metrics);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("LSWorkerStats(");
    boolean first = true;

    if (is_set_time_stamp()) {
      sb.append("time_stamp:");
      sb.append(this.time_stamp);
      first = false;
    }
    if (is_set_metrics()) {
      if (!first) sb.append(", ");
      sb.append("metrics:");
      if (this.metrics == null) {
        sb.append("null");
      } else {
        sb.append(this.metrics);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class LSWorkerStatsStandardSchemeFactory implements SchemeFactory {
    public LSWorkerStatsStandardScheme getScheme() {
      return new LSWorkerStatsStandardScheme();
    }
  }

  private static class LSWorkerStatsStandardScheme extends StandardScheme<LSWorkerStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LSWorkerStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TIME_STAMP
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.time_stamp = iprot.readI64();
              struct.set_time_stamp_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // METRICS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map658 = iprot.readMapBegin();
                struct.metrics = new HashMap<String,Double>(2*_map658.size);
                String _key659;
                double _val660;
                for (int _i661 = 0; _i661 < _map658.size; ++_i661)
                {
                  _key659 = iprot.readString();
                  _val660 = iprot.readDouble();
                  struct.metrics.put(_key659, _val660);
                }
                iprot.readMapEnd();
              }
              struct.set_metrics_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, LSWorkerStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.is_set_time_stamp()) {
        oprot.writeFieldBegin(TIME_STAMP_FIELD_DESC);
        oprot.writeI64(struct.time_stamp);
        oprot.writeFieldEnd();
      }
      if (struct.metrics != null) {
        if (struct.is_set_metrics()) {
          oprot.writeFieldBegin(METRICS_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, struct.metrics.size()));
            for (Map.Entry<String, Double> _iter662 : struct.metrics.entrySet())
            {
              oprot.writeString(_iter662.getKey());
              oprot.writeDouble(_iter662.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LSWorkerStatsTupleSchemeFactory implements SchemeFactory {
    public LSWorkerStatsTupleScheme getScheme() {
      return new LSWorkerStatsTupleScheme();
    }
  }

  private static class LSWorkerStatsTupleScheme extends TupleScheme<LSWorkerStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LSWorkerStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_time_stamp()) {
        optionals.set(0);
      }
      if (struct.is_set_metrics()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.is_set_time_stamp()) {
        oprot.writeI64(struct.time_stamp);
      }
      if (struct.is_set_metrics()) {
        {
          oprot.writeI32(struct.metrics.size());
          for (Map.Entry<String, Double> _iter663 : struct.metrics.entrySet())
          {
            oprot.writeString(_iter663.getKey());
            oprot.writeDouble(_iter663.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LSWorkerStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.time_stamp = iprot.readI64();
        struct.set_time_stamp_isSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map664 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, iprot.readI32());
          struct.metrics = new HashMap<String,Double>(2*_map664.size);
          String _key665;
          double _val666;
          for (int _i667 = 0; _i667 < _map664.size; ++_i667)
          {
            _key665 = iprot.readString();
            _val666 = iprot.readDouble();
            struct.metrics.put(_key665, _val666);
          }
        }
        struct.set_metrics_isSet(true);
      }
    }
  }

}

