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
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package backtype.storm.generated;

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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-5-7")
public class TopologyStats implements org.apache.thrift.TBase<TopologyStats, TopologyStats._Fields>, java.io.Serializable, Cloneable, Comparable<TopologyStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TopologyStats");

  private static final org.apache.thrift.protocol.TField EMITTED_FIELD_DESC = new org.apache.thrift.protocol.TField("emitted", org.apache.thrift.protocol.TType.MAP, (short)513);
  private static final org.apache.thrift.protocol.TField TRANSFERRED_FIELD_DESC = new org.apache.thrift.protocol.TField("transferred", org.apache.thrift.protocol.TType.MAP, (short)514);
  private static final org.apache.thrift.protocol.TField COMPLETE_LATENCIES_FIELD_DESC = new org.apache.thrift.protocol.TField("complete_latencies", org.apache.thrift.protocol.TType.MAP, (short)515);
  private static final org.apache.thrift.protocol.TField ACKED_FIELD_DESC = new org.apache.thrift.protocol.TField("acked", org.apache.thrift.protocol.TType.MAP, (short)516);
  private static final org.apache.thrift.protocol.TField FAILED_FIELD_DESC = new org.apache.thrift.protocol.TField("failed", org.apache.thrift.protocol.TType.MAP, (short)517);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TopologyStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TopologyStatsTupleSchemeFactory());
  }

  private Map<String,Long> emitted; // optional
  private Map<String,Long> transferred; // optional
  private Map<String,Double> complete_latencies; // optional
  private Map<String,Long> acked; // optional
  private Map<String,Long> failed; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EMITTED((short)513, "emitted"),
    TRANSFERRED((short)514, "transferred"),
    COMPLETE_LATENCIES((short)515, "complete_latencies"),
    ACKED((short)516, "acked"),
    FAILED((short)517, "failed");

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
        case 513: // EMITTED
          return EMITTED;
        case 514: // TRANSFERRED
          return TRANSFERRED;
        case 515: // COMPLETE_LATENCIES
          return COMPLETE_LATENCIES;
        case 516: // ACKED
          return ACKED;
        case 517: // FAILED
          return FAILED;
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
  private static final _Fields optionals[] = {_Fields.EMITTED,_Fields.TRANSFERRED,_Fields.COMPLETE_LATENCIES,_Fields.ACKED,_Fields.FAILED};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.EMITTED, new org.apache.thrift.meta_data.FieldMetaData("emitted", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.TRANSFERRED, new org.apache.thrift.meta_data.FieldMetaData("transferred", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.COMPLETE_LATENCIES, new org.apache.thrift.meta_data.FieldMetaData("complete_latencies", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE))));
    tmpMap.put(_Fields.ACKED, new org.apache.thrift.meta_data.FieldMetaData("acked", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    tmpMap.put(_Fields.FAILED, new org.apache.thrift.meta_data.FieldMetaData("failed", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TopologyStats.class, metaDataMap);
  }

  public TopologyStats() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TopologyStats(TopologyStats other) {
    if (other.is_set_emitted()) {
      Map<String,Long> __this__emitted = new HashMap<String,Long>(other.emitted);
      this.emitted = __this__emitted;
    }
    if (other.is_set_transferred()) {
      Map<String,Long> __this__transferred = new HashMap<String,Long>(other.transferred);
      this.transferred = __this__transferred;
    }
    if (other.is_set_complete_latencies()) {
      Map<String,Double> __this__complete_latencies = new HashMap<String,Double>(other.complete_latencies);
      this.complete_latencies = __this__complete_latencies;
    }
    if (other.is_set_acked()) {
      Map<String,Long> __this__acked = new HashMap<String,Long>(other.acked);
      this.acked = __this__acked;
    }
    if (other.is_set_failed()) {
      Map<String,Long> __this__failed = new HashMap<String,Long>(other.failed);
      this.failed = __this__failed;
    }
  }

  public TopologyStats deepCopy() {
    return new TopologyStats(this);
  }

  @Override
  public void clear() {
    this.emitted = null;
    this.transferred = null;
    this.complete_latencies = null;
    this.acked = null;
    this.failed = null;
  }

  public int get_emitted_size() {
    return (this.emitted == null) ? 0 : this.emitted.size();
  }

  public void put_to_emitted(String key, long val) {
    if (this.emitted == null) {
      this.emitted = new HashMap<String,Long>();
    }
    this.emitted.put(key, val);
  }

  public Map<String,Long> get_emitted() {
    return this.emitted;
  }

  public void set_emitted(Map<String,Long> emitted) {
    this.emitted = emitted;
  }

  public void unset_emitted() {
    this.emitted = null;
  }

  /** Returns true if field emitted is set (has been assigned a value) and false otherwise */
  public boolean is_set_emitted() {
    return this.emitted != null;
  }

  public void set_emitted_isSet(boolean value) {
    if (!value) {
      this.emitted = null;
    }
  }

  public int get_transferred_size() {
    return (this.transferred == null) ? 0 : this.transferred.size();
  }

  public void put_to_transferred(String key, long val) {
    if (this.transferred == null) {
      this.transferred = new HashMap<String,Long>();
    }
    this.transferred.put(key, val);
  }

  public Map<String,Long> get_transferred() {
    return this.transferred;
  }

  public void set_transferred(Map<String,Long> transferred) {
    this.transferred = transferred;
  }

  public void unset_transferred() {
    this.transferred = null;
  }

  /** Returns true if field transferred is set (has been assigned a value) and false otherwise */
  public boolean is_set_transferred() {
    return this.transferred != null;
  }

  public void set_transferred_isSet(boolean value) {
    if (!value) {
      this.transferred = null;
    }
  }

  public int get_complete_latencies_size() {
    return (this.complete_latencies == null) ? 0 : this.complete_latencies.size();
  }

  public void put_to_complete_latencies(String key, double val) {
    if (this.complete_latencies == null) {
      this.complete_latencies = new HashMap<String,Double>();
    }
    this.complete_latencies.put(key, val);
  }

  public Map<String,Double> get_complete_latencies() {
    return this.complete_latencies;
  }

  public void set_complete_latencies(Map<String,Double> complete_latencies) {
    this.complete_latencies = complete_latencies;
  }

  public void unset_complete_latencies() {
    this.complete_latencies = null;
  }

  /** Returns true if field complete_latencies is set (has been assigned a value) and false otherwise */
  public boolean is_set_complete_latencies() {
    return this.complete_latencies != null;
  }

  public void set_complete_latencies_isSet(boolean value) {
    if (!value) {
      this.complete_latencies = null;
    }
  }

  public int get_acked_size() {
    return (this.acked == null) ? 0 : this.acked.size();
  }

  public void put_to_acked(String key, long val) {
    if (this.acked == null) {
      this.acked = new HashMap<String,Long>();
    }
    this.acked.put(key, val);
  }

  public Map<String,Long> get_acked() {
    return this.acked;
  }

  public void set_acked(Map<String,Long> acked) {
    this.acked = acked;
  }

  public void unset_acked() {
    this.acked = null;
  }

  /** Returns true if field acked is set (has been assigned a value) and false otherwise */
  public boolean is_set_acked() {
    return this.acked != null;
  }

  public void set_acked_isSet(boolean value) {
    if (!value) {
      this.acked = null;
    }
  }

  public int get_failed_size() {
    return (this.failed == null) ? 0 : this.failed.size();
  }

  public void put_to_failed(String key, long val) {
    if (this.failed == null) {
      this.failed = new HashMap<String,Long>();
    }
    this.failed.put(key, val);
  }

  public Map<String,Long> get_failed() {
    return this.failed;
  }

  public void set_failed(Map<String,Long> failed) {
    this.failed = failed;
  }

  public void unset_failed() {
    this.failed = null;
  }

  /** Returns true if field failed is set (has been assigned a value) and false otherwise */
  public boolean is_set_failed() {
    return this.failed != null;
  }

  public void set_failed_isSet(boolean value) {
    if (!value) {
      this.failed = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case EMITTED:
      if (value == null) {
        unset_emitted();
      } else {
        set_emitted((Map<String,Long>)value);
      }
      break;

    case TRANSFERRED:
      if (value == null) {
        unset_transferred();
      } else {
        set_transferred((Map<String,Long>)value);
      }
      break;

    case COMPLETE_LATENCIES:
      if (value == null) {
        unset_complete_latencies();
      } else {
        set_complete_latencies((Map<String,Double>)value);
      }
      break;

    case ACKED:
      if (value == null) {
        unset_acked();
      } else {
        set_acked((Map<String,Long>)value);
      }
      break;

    case FAILED:
      if (value == null) {
        unset_failed();
      } else {
        set_failed((Map<String,Long>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case EMITTED:
      return get_emitted();

    case TRANSFERRED:
      return get_transferred();

    case COMPLETE_LATENCIES:
      return get_complete_latencies();

    case ACKED:
      return get_acked();

    case FAILED:
      return get_failed();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case EMITTED:
      return is_set_emitted();
    case TRANSFERRED:
      return is_set_transferred();
    case COMPLETE_LATENCIES:
      return is_set_complete_latencies();
    case ACKED:
      return is_set_acked();
    case FAILED:
      return is_set_failed();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TopologyStats)
      return this.equals((TopologyStats)that);
    return false;
  }

  public boolean equals(TopologyStats that) {
    if (that == null)
      return false;

    boolean this_present_emitted = true && this.is_set_emitted();
    boolean that_present_emitted = true && that.is_set_emitted();
    if (this_present_emitted || that_present_emitted) {
      if (!(this_present_emitted && that_present_emitted))
        return false;
      if (!this.emitted.equals(that.emitted))
        return false;
    }

    boolean this_present_transferred = true && this.is_set_transferred();
    boolean that_present_transferred = true && that.is_set_transferred();
    if (this_present_transferred || that_present_transferred) {
      if (!(this_present_transferred && that_present_transferred))
        return false;
      if (!this.transferred.equals(that.transferred))
        return false;
    }

    boolean this_present_complete_latencies = true && this.is_set_complete_latencies();
    boolean that_present_complete_latencies = true && that.is_set_complete_latencies();
    if (this_present_complete_latencies || that_present_complete_latencies) {
      if (!(this_present_complete_latencies && that_present_complete_latencies))
        return false;
      if (!this.complete_latencies.equals(that.complete_latencies))
        return false;
    }

    boolean this_present_acked = true && this.is_set_acked();
    boolean that_present_acked = true && that.is_set_acked();
    if (this_present_acked || that_present_acked) {
      if (!(this_present_acked && that_present_acked))
        return false;
      if (!this.acked.equals(that.acked))
        return false;
    }

    boolean this_present_failed = true && this.is_set_failed();
    boolean that_present_failed = true && that.is_set_failed();
    if (this_present_failed || that_present_failed) {
      if (!(this_present_failed && that_present_failed))
        return false;
      if (!this.failed.equals(that.failed))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_emitted = true && (is_set_emitted());
    list.add(present_emitted);
    if (present_emitted)
      list.add(emitted);

    boolean present_transferred = true && (is_set_transferred());
    list.add(present_transferred);
    if (present_transferred)
      list.add(transferred);

    boolean present_complete_latencies = true && (is_set_complete_latencies());
    list.add(present_complete_latencies);
    if (present_complete_latencies)
      list.add(complete_latencies);

    boolean present_acked = true && (is_set_acked());
    list.add(present_acked);
    if (present_acked)
      list.add(acked);

    boolean present_failed = true && (is_set_failed());
    list.add(present_failed);
    if (present_failed)
      list.add(failed);

    return list.hashCode();
  }

  @Override
  public int compareTo(TopologyStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_emitted()).compareTo(other.is_set_emitted());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_emitted()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.emitted, other.emitted);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_transferred()).compareTo(other.is_set_transferred());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_transferred()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.transferred, other.transferred);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_complete_latencies()).compareTo(other.is_set_complete_latencies());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_complete_latencies()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.complete_latencies, other.complete_latencies);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_acked()).compareTo(other.is_set_acked());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_acked()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.acked, other.acked);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_failed()).compareTo(other.is_set_failed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_failed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.failed, other.failed);
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
    StringBuilder sb = new StringBuilder("TopologyStats(");
    boolean first = true;

    if (is_set_emitted()) {
      sb.append("emitted:");
      if (this.emitted == null) {
        sb.append("null");
      } else {
        sb.append(this.emitted);
      }
      first = false;
    }
    if (is_set_transferred()) {
      if (!first) sb.append(", ");
      sb.append("transferred:");
      if (this.transferred == null) {
        sb.append("null");
      } else {
        sb.append(this.transferred);
      }
      first = false;
    }
    if (is_set_complete_latencies()) {
      if (!first) sb.append(", ");
      sb.append("complete_latencies:");
      if (this.complete_latencies == null) {
        sb.append("null");
      } else {
        sb.append(this.complete_latencies);
      }
      first = false;
    }
    if (is_set_acked()) {
      if (!first) sb.append(", ");
      sb.append("acked:");
      if (this.acked == null) {
        sb.append("null");
      } else {
        sb.append(this.acked);
      }
      first = false;
    }
    if (is_set_failed()) {
      if (!first) sb.append(", ");
      sb.append("failed:");
      if (this.failed == null) {
        sb.append("null");
      } else {
        sb.append(this.failed);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TopologyStatsStandardSchemeFactory implements SchemeFactory {
    public TopologyStatsStandardScheme getScheme() {
      return new TopologyStatsStandardScheme();
    }
  }

  private static class TopologyStatsStandardScheme extends StandardScheme<TopologyStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TopologyStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 513: // EMITTED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map316 = iprot.readMapBegin();
                struct.emitted = new HashMap<String,Long>(2*_map316.size);
                String _key317;
                long _val318;
                for (int _i319 = 0; _i319 < _map316.size; ++_i319)
                {
                  _key317 = iprot.readString();
                  _val318 = iprot.readI64();
                  struct.emitted.put(_key317, _val318);
                }
                iprot.readMapEnd();
              }
              struct.set_emitted_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 514: // TRANSFERRED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map320 = iprot.readMapBegin();
                struct.transferred = new HashMap<String,Long>(2*_map320.size);
                String _key321;
                long _val322;
                for (int _i323 = 0; _i323 < _map320.size; ++_i323)
                {
                  _key321 = iprot.readString();
                  _val322 = iprot.readI64();
                  struct.transferred.put(_key321, _val322);
                }
                iprot.readMapEnd();
              }
              struct.set_transferred_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 515: // COMPLETE_LATENCIES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map324 = iprot.readMapBegin();
                struct.complete_latencies = new HashMap<String,Double>(2*_map324.size);
                String _key325;
                double _val326;
                for (int _i327 = 0; _i327 < _map324.size; ++_i327)
                {
                  _key325 = iprot.readString();
                  _val326 = iprot.readDouble();
                  struct.complete_latencies.put(_key325, _val326);
                }
                iprot.readMapEnd();
              }
              struct.set_complete_latencies_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 516: // ACKED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map328 = iprot.readMapBegin();
                struct.acked = new HashMap<String,Long>(2*_map328.size);
                String _key329;
                long _val330;
                for (int _i331 = 0; _i331 < _map328.size; ++_i331)
                {
                  _key329 = iprot.readString();
                  _val330 = iprot.readI64();
                  struct.acked.put(_key329, _val330);
                }
                iprot.readMapEnd();
              }
              struct.set_acked_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 517: // FAILED
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map332 = iprot.readMapBegin();
                struct.failed = new HashMap<String,Long>(2*_map332.size);
                String _key333;
                long _val334;
                for (int _i335 = 0; _i335 < _map332.size; ++_i335)
                {
                  _key333 = iprot.readString();
                  _val334 = iprot.readI64();
                  struct.failed.put(_key333, _val334);
                }
                iprot.readMapEnd();
              }
              struct.set_failed_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TopologyStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.emitted != null) {
        if (struct.is_set_emitted()) {
          oprot.writeFieldBegin(EMITTED_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, struct.emitted.size()));
            for (Map.Entry<String, Long> _iter336 : struct.emitted.entrySet())
            {
              oprot.writeString(_iter336.getKey());
              oprot.writeI64(_iter336.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.transferred != null) {
        if (struct.is_set_transferred()) {
          oprot.writeFieldBegin(TRANSFERRED_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, struct.transferred.size()));
            for (Map.Entry<String, Long> _iter337 : struct.transferred.entrySet())
            {
              oprot.writeString(_iter337.getKey());
              oprot.writeI64(_iter337.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.complete_latencies != null) {
        if (struct.is_set_complete_latencies()) {
          oprot.writeFieldBegin(COMPLETE_LATENCIES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, struct.complete_latencies.size()));
            for (Map.Entry<String, Double> _iter338 : struct.complete_latencies.entrySet())
            {
              oprot.writeString(_iter338.getKey());
              oprot.writeDouble(_iter338.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.acked != null) {
        if (struct.is_set_acked()) {
          oprot.writeFieldBegin(ACKED_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, struct.acked.size()));
            for (Map.Entry<String, Long> _iter339 : struct.acked.entrySet())
            {
              oprot.writeString(_iter339.getKey());
              oprot.writeI64(_iter339.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.failed != null) {
        if (struct.is_set_failed()) {
          oprot.writeFieldBegin(FAILED_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, struct.failed.size()));
            for (Map.Entry<String, Long> _iter340 : struct.failed.entrySet())
            {
              oprot.writeString(_iter340.getKey());
              oprot.writeI64(_iter340.getValue());
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

  private static class TopologyStatsTupleSchemeFactory implements SchemeFactory {
    public TopologyStatsTupleScheme getScheme() {
      return new TopologyStatsTupleScheme();
    }
  }

  private static class TopologyStatsTupleScheme extends TupleScheme<TopologyStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TopologyStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_emitted()) {
        optionals.set(0);
      }
      if (struct.is_set_transferred()) {
        optionals.set(1);
      }
      if (struct.is_set_complete_latencies()) {
        optionals.set(2);
      }
      if (struct.is_set_acked()) {
        optionals.set(3);
      }
      if (struct.is_set_failed()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.is_set_emitted()) {
        {
          oprot.writeI32(struct.emitted.size());
          for (Map.Entry<String, Long> _iter341 : struct.emitted.entrySet())
          {
            oprot.writeString(_iter341.getKey());
            oprot.writeI64(_iter341.getValue());
          }
        }
      }
      if (struct.is_set_transferred()) {
        {
          oprot.writeI32(struct.transferred.size());
          for (Map.Entry<String, Long> _iter342 : struct.transferred.entrySet())
          {
            oprot.writeString(_iter342.getKey());
            oprot.writeI64(_iter342.getValue());
          }
        }
      }
      if (struct.is_set_complete_latencies()) {
        {
          oprot.writeI32(struct.complete_latencies.size());
          for (Map.Entry<String, Double> _iter343 : struct.complete_latencies.entrySet())
          {
            oprot.writeString(_iter343.getKey());
            oprot.writeDouble(_iter343.getValue());
          }
        }
      }
      if (struct.is_set_acked()) {
        {
          oprot.writeI32(struct.acked.size());
          for (Map.Entry<String, Long> _iter344 : struct.acked.entrySet())
          {
            oprot.writeString(_iter344.getKey());
            oprot.writeI64(_iter344.getValue());
          }
        }
      }
      if (struct.is_set_failed()) {
        {
          oprot.writeI32(struct.failed.size());
          for (Map.Entry<String, Long> _iter345 : struct.failed.entrySet())
          {
            oprot.writeString(_iter345.getKey());
            oprot.writeI64(_iter345.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TopologyStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map346 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.emitted = new HashMap<String,Long>(2*_map346.size);
          String _key347;
          long _val348;
          for (int _i349 = 0; _i349 < _map346.size; ++_i349)
          {
            _key347 = iprot.readString();
            _val348 = iprot.readI64();
            struct.emitted.put(_key347, _val348);
          }
        }
        struct.set_emitted_isSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TMap _map350 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.transferred = new HashMap<String,Long>(2*_map350.size);
          String _key351;
          long _val352;
          for (int _i353 = 0; _i353 < _map350.size; ++_i353)
          {
            _key351 = iprot.readString();
            _val352 = iprot.readI64();
            struct.transferred.put(_key351, _val352);
          }
        }
        struct.set_transferred_isSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map354 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, iprot.readI32());
          struct.complete_latencies = new HashMap<String,Double>(2*_map354.size);
          String _key355;
          double _val356;
          for (int _i357 = 0; _i357 < _map354.size; ++_i357)
          {
            _key355 = iprot.readString();
            _val356 = iprot.readDouble();
            struct.complete_latencies.put(_key355, _val356);
          }
        }
        struct.set_complete_latencies_isSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TMap _map358 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.acked = new HashMap<String,Long>(2*_map358.size);
          String _key359;
          long _val360;
          for (int _i361 = 0; _i361 < _map358.size; ++_i361)
          {
            _key359 = iprot.readString();
            _val360 = iprot.readI64();
            struct.acked.put(_key359, _val360);
          }
        }
        struct.set_acked_isSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TMap _map362 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.failed = new HashMap<String,Long>(2*_map362.size);
          String _key363;
          long _val364;
          for (int _i365 = 0; _i365 < _map362.size; ++_i365)
          {
            _key363 = iprot.readString();
            _val364 = iprot.readI64();
            struct.failed.put(_key363, _val364);
          }
        }
        struct.set_failed_isSet(true);
      }
    }
  }

}

