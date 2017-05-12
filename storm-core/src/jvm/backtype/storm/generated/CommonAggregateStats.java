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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
public class CommonAggregateStats implements org.apache.thrift.TBase<CommonAggregateStats, CommonAggregateStats._Fields>, java.io.Serializable, Cloneable, Comparable<CommonAggregateStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CommonAggregateStats");

  private static final org.apache.thrift.protocol.TField NUM_EXECUTORS_FIELD_DESC = new org.apache.thrift.protocol.TField("num_executors", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField NUM_TASKS_FIELD_DESC = new org.apache.thrift.protocol.TField("num_tasks", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField EMITTED_FIELD_DESC = new org.apache.thrift.protocol.TField("emitted", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField TRANSFERRED_FIELD_DESC = new org.apache.thrift.protocol.TField("transferred", org.apache.thrift.protocol.TType.I64, (short)4);
  private static final org.apache.thrift.protocol.TField ACKED_FIELD_DESC = new org.apache.thrift.protocol.TField("acked", org.apache.thrift.protocol.TType.I64, (short)5);
  private static final org.apache.thrift.protocol.TField FAILED_FIELD_DESC = new org.apache.thrift.protocol.TField("failed", org.apache.thrift.protocol.TType.I64, (short)6);
  private static final org.apache.thrift.protocol.TField RESOURCES_MAP_FIELD_DESC = new org.apache.thrift.protocol.TField("resources_map", org.apache.thrift.protocol.TType.MAP, (short)7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new CommonAggregateStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new CommonAggregateStatsTupleSchemeFactory());
  }

  private int num_executors; // optional
  private int num_tasks; // optional
  private long emitted; // optional
  private long transferred; // optional
  private long acked; // optional
  private long failed; // optional
  private Map<String,Double> resources_map; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NUM_EXECUTORS((short)1, "num_executors"),
    NUM_TASKS((short)2, "num_tasks"),
    EMITTED((short)3, "emitted"),
    TRANSFERRED((short)4, "transferred"),
    ACKED((short)5, "acked"),
    FAILED((short)6, "failed"),
    RESOURCES_MAP((short)7, "resources_map");

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
        case 1: // NUM_EXECUTORS
          return NUM_EXECUTORS;
        case 2: // NUM_TASKS
          return NUM_TASKS;
        case 3: // EMITTED
          return EMITTED;
        case 4: // TRANSFERRED
          return TRANSFERRED;
        case 5: // ACKED
          return ACKED;
        case 6: // FAILED
          return FAILED;
        case 7: // RESOURCES_MAP
          return RESOURCES_MAP;
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
  private static final int __NUM_EXECUTORS_ISSET_ID = 0;
  private static final int __NUM_TASKS_ISSET_ID = 1;
  private static final int __EMITTED_ISSET_ID = 2;
  private static final int __TRANSFERRED_ISSET_ID = 3;
  private static final int __ACKED_ISSET_ID = 4;
  private static final int __FAILED_ISSET_ID = 5;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.NUM_EXECUTORS,_Fields.NUM_TASKS,_Fields.EMITTED,_Fields.TRANSFERRED,_Fields.ACKED,_Fields.FAILED,_Fields.RESOURCES_MAP};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NUM_EXECUTORS, new org.apache.thrift.meta_data.FieldMetaData("num_executors", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_TASKS, new org.apache.thrift.meta_data.FieldMetaData("num_tasks", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.EMITTED, new org.apache.thrift.meta_data.FieldMetaData("emitted", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.TRANSFERRED, new org.apache.thrift.meta_data.FieldMetaData("transferred", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.ACKED, new org.apache.thrift.meta_data.FieldMetaData("acked", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.FAILED, new org.apache.thrift.meta_data.FieldMetaData("failed", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.RESOURCES_MAP, new org.apache.thrift.meta_data.FieldMetaData("resources_map", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CommonAggregateStats.class, metaDataMap);
  }

  public CommonAggregateStats() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public CommonAggregateStats(CommonAggregateStats other) {
    __isset_bitfield = other.__isset_bitfield;
    this.num_executors = other.num_executors;
    this.num_tasks = other.num_tasks;
    this.emitted = other.emitted;
    this.transferred = other.transferred;
    this.acked = other.acked;
    this.failed = other.failed;
    if (other.is_set_resources_map()) {
      Map<String,Double> __this__resources_map = new HashMap<String,Double>(other.resources_map);
      this.resources_map = __this__resources_map;
    }
  }

  public CommonAggregateStats deepCopy() {
    return new CommonAggregateStats(this);
  }

  @Override
  public void clear() {
    set_num_executors_isSet(false);
    this.num_executors = 0;
    set_num_tasks_isSet(false);
    this.num_tasks = 0;
    set_emitted_isSet(false);
    this.emitted = 0;
    set_transferred_isSet(false);
    this.transferred = 0;
    set_acked_isSet(false);
    this.acked = 0;
    set_failed_isSet(false);
    this.failed = 0;
    this.resources_map = null;
  }

  public int get_num_executors() {
    return this.num_executors;
  }

  public void set_num_executors(int num_executors) {
    this.num_executors = num_executors;
    set_num_executors_isSet(true);
  }

  public void unset_num_executors() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_EXECUTORS_ISSET_ID);
  }

  /** Returns true if field num_executors is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_executors() {
    return EncodingUtils.testBit(__isset_bitfield, __NUM_EXECUTORS_ISSET_ID);
  }

  public void set_num_executors_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_EXECUTORS_ISSET_ID, value);
  }

  public int get_num_tasks() {
    return this.num_tasks;
  }

  public void set_num_tasks(int num_tasks) {
    this.num_tasks = num_tasks;
    set_num_tasks_isSet(true);
  }

  public void unset_num_tasks() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __NUM_TASKS_ISSET_ID);
  }

  /** Returns true if field num_tasks is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_tasks() {
    return EncodingUtils.testBit(__isset_bitfield, __NUM_TASKS_ISSET_ID);
  }

  public void set_num_tasks_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __NUM_TASKS_ISSET_ID, value);
  }

  public long get_emitted() {
    return this.emitted;
  }

  public void set_emitted(long emitted) {
    this.emitted = emitted;
    set_emitted_isSet(true);
  }

  public void unset_emitted() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __EMITTED_ISSET_ID);
  }

  /** Returns true if field emitted is set (has been assigned a value) and false otherwise */
  public boolean is_set_emitted() {
    return EncodingUtils.testBit(__isset_bitfield, __EMITTED_ISSET_ID);
  }

  public void set_emitted_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __EMITTED_ISSET_ID, value);
  }

  public long get_transferred() {
    return this.transferred;
  }

  public void set_transferred(long transferred) {
    this.transferred = transferred;
    set_transferred_isSet(true);
  }

  public void unset_transferred() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __TRANSFERRED_ISSET_ID);
  }

  /** Returns true if field transferred is set (has been assigned a value) and false otherwise */
  public boolean is_set_transferred() {
    return EncodingUtils.testBit(__isset_bitfield, __TRANSFERRED_ISSET_ID);
  }

  public void set_transferred_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __TRANSFERRED_ISSET_ID, value);
  }

  public long get_acked() {
    return this.acked;
  }

  public void set_acked(long acked) {
    this.acked = acked;
    set_acked_isSet(true);
  }

  public void unset_acked() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ACKED_ISSET_ID);
  }

  /** Returns true if field acked is set (has been assigned a value) and false otherwise */
  public boolean is_set_acked() {
    return EncodingUtils.testBit(__isset_bitfield, __ACKED_ISSET_ID);
  }

  public void set_acked_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ACKED_ISSET_ID, value);
  }

  public long get_failed() {
    return this.failed;
  }

  public void set_failed(long failed) {
    this.failed = failed;
    set_failed_isSet(true);
  }

  public void unset_failed() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __FAILED_ISSET_ID);
  }

  /** Returns true if field failed is set (has been assigned a value) and false otherwise */
  public boolean is_set_failed() {
    return EncodingUtils.testBit(__isset_bitfield, __FAILED_ISSET_ID);
  }

  public void set_failed_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __FAILED_ISSET_ID, value);
  }

  public int get_resources_map_size() {
    return (this.resources_map == null) ? 0 : this.resources_map.size();
  }

  public void put_to_resources_map(String key, double val) {
    if (this.resources_map == null) {
      this.resources_map = new HashMap<String,Double>();
    }
    this.resources_map.put(key, val);
  }

  public Map<String,Double> get_resources_map() {
    return this.resources_map;
  }

  public void set_resources_map(Map<String,Double> resources_map) {
    this.resources_map = resources_map;
  }

  public void unset_resources_map() {
    this.resources_map = null;
  }

  /** Returns true if field resources_map is set (has been assigned a value) and false otherwise */
  public boolean is_set_resources_map() {
    return this.resources_map != null;
  }

  public void set_resources_map_isSet(boolean value) {
    if (!value) {
      this.resources_map = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NUM_EXECUTORS:
      if (value == null) {
        unset_num_executors();
      } else {
        set_num_executors((Integer)value);
      }
      break;

    case NUM_TASKS:
      if (value == null) {
        unset_num_tasks();
      } else {
        set_num_tasks((Integer)value);
      }
      break;

    case EMITTED:
      if (value == null) {
        unset_emitted();
      } else {
        set_emitted((Long)value);
      }
      break;

    case TRANSFERRED:
      if (value == null) {
        unset_transferred();
      } else {
        set_transferred((Long)value);
      }
      break;

    case ACKED:
      if (value == null) {
        unset_acked();
      } else {
        set_acked((Long)value);
      }
      break;

    case FAILED:
      if (value == null) {
        unset_failed();
      } else {
        set_failed((Long)value);
      }
      break;

    case RESOURCES_MAP:
      if (value == null) {
        unset_resources_map();
      } else {
        set_resources_map((Map<String,Double>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NUM_EXECUTORS:
      return get_num_executors();

    case NUM_TASKS:
      return get_num_tasks();

    case EMITTED:
      return get_emitted();

    case TRANSFERRED:
      return get_transferred();

    case ACKED:
      return get_acked();

    case FAILED:
      return get_failed();

    case RESOURCES_MAP:
      return get_resources_map();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NUM_EXECUTORS:
      return is_set_num_executors();
    case NUM_TASKS:
      return is_set_num_tasks();
    case EMITTED:
      return is_set_emitted();
    case TRANSFERRED:
      return is_set_transferred();
    case ACKED:
      return is_set_acked();
    case FAILED:
      return is_set_failed();
    case RESOURCES_MAP:
      return is_set_resources_map();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof CommonAggregateStats)
      return this.equals((CommonAggregateStats)that);
    return false;
  }

  public boolean equals(CommonAggregateStats that) {
    if (that == null)
      return false;

    boolean this_present_num_executors = true && this.is_set_num_executors();
    boolean that_present_num_executors = true && that.is_set_num_executors();
    if (this_present_num_executors || that_present_num_executors) {
      if (!(this_present_num_executors && that_present_num_executors))
        return false;
      if (this.num_executors != that.num_executors)
        return false;
    }

    boolean this_present_num_tasks = true && this.is_set_num_tasks();
    boolean that_present_num_tasks = true && that.is_set_num_tasks();
    if (this_present_num_tasks || that_present_num_tasks) {
      if (!(this_present_num_tasks && that_present_num_tasks))
        return false;
      if (this.num_tasks != that.num_tasks)
        return false;
    }

    boolean this_present_emitted = true && this.is_set_emitted();
    boolean that_present_emitted = true && that.is_set_emitted();
    if (this_present_emitted || that_present_emitted) {
      if (!(this_present_emitted && that_present_emitted))
        return false;
      if (this.emitted != that.emitted)
        return false;
    }

    boolean this_present_transferred = true && this.is_set_transferred();
    boolean that_present_transferred = true && that.is_set_transferred();
    if (this_present_transferred || that_present_transferred) {
      if (!(this_present_transferred && that_present_transferred))
        return false;
      if (this.transferred != that.transferred)
        return false;
    }

    boolean this_present_acked = true && this.is_set_acked();
    boolean that_present_acked = true && that.is_set_acked();
    if (this_present_acked || that_present_acked) {
      if (!(this_present_acked && that_present_acked))
        return false;
      if (this.acked != that.acked)
        return false;
    }

    boolean this_present_failed = true && this.is_set_failed();
    boolean that_present_failed = true && that.is_set_failed();
    if (this_present_failed || that_present_failed) {
      if (!(this_present_failed && that_present_failed))
        return false;
      if (this.failed != that.failed)
        return false;
    }

    boolean this_present_resources_map = true && this.is_set_resources_map();
    boolean that_present_resources_map = true && that.is_set_resources_map();
    if (this_present_resources_map || that_present_resources_map) {
      if (!(this_present_resources_map && that_present_resources_map))
        return false;
      if (!this.resources_map.equals(that.resources_map))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_num_executors = true && (is_set_num_executors());
    list.add(present_num_executors);
    if (present_num_executors)
      list.add(num_executors);

    boolean present_num_tasks = true && (is_set_num_tasks());
    list.add(present_num_tasks);
    if (present_num_tasks)
      list.add(num_tasks);

    boolean present_emitted = true && (is_set_emitted());
    list.add(present_emitted);
    if (present_emitted)
      list.add(emitted);

    boolean present_transferred = true && (is_set_transferred());
    list.add(present_transferred);
    if (present_transferred)
      list.add(transferred);

    boolean present_acked = true && (is_set_acked());
    list.add(present_acked);
    if (present_acked)
      list.add(acked);

    boolean present_failed = true && (is_set_failed());
    list.add(present_failed);
    if (present_failed)
      list.add(failed);

    boolean present_resources_map = true && (is_set_resources_map());
    list.add(present_resources_map);
    if (present_resources_map)
      list.add(resources_map);

    return list.hashCode();
  }

  @Override
  public int compareTo(CommonAggregateStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_num_executors()).compareTo(other.is_set_num_executors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_executors()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_executors, other.num_executors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_num_tasks()).compareTo(other.is_set_num_tasks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_tasks()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.num_tasks, other.num_tasks);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
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
    lastComparison = Boolean.valueOf(is_set_resources_map()).compareTo(other.is_set_resources_map());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_resources_map()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.resources_map, other.resources_map);
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
    StringBuilder sb = new StringBuilder("CommonAggregateStats(");
    boolean first = true;

    if (is_set_num_executors()) {
      sb.append("num_executors:");
      sb.append(this.num_executors);
      first = false;
    }
    if (is_set_num_tasks()) {
      if (!first) sb.append(", ");
      sb.append("num_tasks:");
      sb.append(this.num_tasks);
      first = false;
    }
    if (is_set_emitted()) {
      if (!first) sb.append(", ");
      sb.append("emitted:");
      sb.append(this.emitted);
      first = false;
    }
    if (is_set_transferred()) {
      if (!first) sb.append(", ");
      sb.append("transferred:");
      sb.append(this.transferred);
      first = false;
    }
    if (is_set_acked()) {
      if (!first) sb.append(", ");
      sb.append("acked:");
      sb.append(this.acked);
      first = false;
    }
    if (is_set_failed()) {
      if (!first) sb.append(", ");
      sb.append("failed:");
      sb.append(this.failed);
      first = false;
    }
    if (is_set_resources_map()) {
      if (!first) sb.append(", ");
      sb.append("resources_map:");
      if (this.resources_map == null) {
        sb.append("null");
      } else {
        sb.append(this.resources_map);
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

  private static class CommonAggregateStatsStandardSchemeFactory implements SchemeFactory {
    public CommonAggregateStatsStandardScheme getScheme() {
      return new CommonAggregateStatsStandardScheme();
    }
  }

  private static class CommonAggregateStatsStandardScheme extends StandardScheme<CommonAggregateStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, CommonAggregateStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NUM_EXECUTORS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.num_executors = iprot.readI32();
              struct.set_num_executors_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // NUM_TASKS
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.num_tasks = iprot.readI32();
              struct.set_num_tasks_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // EMITTED
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.emitted = iprot.readI64();
              struct.set_emitted_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // TRANSFERRED
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.transferred = iprot.readI64();
              struct.set_transferred_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // ACKED
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.acked = iprot.readI64();
              struct.set_acked_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // FAILED
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.failed = iprot.readI64();
              struct.set_failed_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // RESOURCES_MAP
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map354 = iprot.readMapBegin();
                struct.resources_map = new HashMap<String,Double>(2*_map354.size);
                String _key355;
                double _val356;
                for (int _i357 = 0; _i357 < _map354.size; ++_i357)
                {
                  _key355 = iprot.readString();
                  _val356 = iprot.readDouble();
                  struct.resources_map.put(_key355, _val356);
                }
                iprot.readMapEnd();
              }
              struct.set_resources_map_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, CommonAggregateStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.is_set_num_executors()) {
        oprot.writeFieldBegin(NUM_EXECUTORS_FIELD_DESC);
        oprot.writeI32(struct.num_executors);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_num_tasks()) {
        oprot.writeFieldBegin(NUM_TASKS_FIELD_DESC);
        oprot.writeI32(struct.num_tasks);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_emitted()) {
        oprot.writeFieldBegin(EMITTED_FIELD_DESC);
        oprot.writeI64(struct.emitted);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_transferred()) {
        oprot.writeFieldBegin(TRANSFERRED_FIELD_DESC);
        oprot.writeI64(struct.transferred);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_acked()) {
        oprot.writeFieldBegin(ACKED_FIELD_DESC);
        oprot.writeI64(struct.acked);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_failed()) {
        oprot.writeFieldBegin(FAILED_FIELD_DESC);
        oprot.writeI64(struct.failed);
        oprot.writeFieldEnd();
      }
      if (struct.resources_map != null) {
        if (struct.is_set_resources_map()) {
          oprot.writeFieldBegin(RESOURCES_MAP_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, struct.resources_map.size()));
            for (Map.Entry<String, Double> _iter358 : struct.resources_map.entrySet())
            {
              oprot.writeString(_iter358.getKey());
              oprot.writeDouble(_iter358.getValue());
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

  private static class CommonAggregateStatsTupleSchemeFactory implements SchemeFactory {
    public CommonAggregateStatsTupleScheme getScheme() {
      return new CommonAggregateStatsTupleScheme();
    }
  }

  private static class CommonAggregateStatsTupleScheme extends TupleScheme<CommonAggregateStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, CommonAggregateStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_num_executors()) {
        optionals.set(0);
      }
      if (struct.is_set_num_tasks()) {
        optionals.set(1);
      }
      if (struct.is_set_emitted()) {
        optionals.set(2);
      }
      if (struct.is_set_transferred()) {
        optionals.set(3);
      }
      if (struct.is_set_acked()) {
        optionals.set(4);
      }
      if (struct.is_set_failed()) {
        optionals.set(5);
      }
      if (struct.is_set_resources_map()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.is_set_num_executors()) {
        oprot.writeI32(struct.num_executors);
      }
      if (struct.is_set_num_tasks()) {
        oprot.writeI32(struct.num_tasks);
      }
      if (struct.is_set_emitted()) {
        oprot.writeI64(struct.emitted);
      }
      if (struct.is_set_transferred()) {
        oprot.writeI64(struct.transferred);
      }
      if (struct.is_set_acked()) {
        oprot.writeI64(struct.acked);
      }
      if (struct.is_set_failed()) {
        oprot.writeI64(struct.failed);
      }
      if (struct.is_set_resources_map()) {
        {
          oprot.writeI32(struct.resources_map.size());
          for (Map.Entry<String, Double> _iter359 : struct.resources_map.entrySet())
          {
            oprot.writeString(_iter359.getKey());
            oprot.writeDouble(_iter359.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, CommonAggregateStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.num_executors = iprot.readI32();
        struct.set_num_executors_isSet(true);
      }
      if (incoming.get(1)) {
        struct.num_tasks = iprot.readI32();
        struct.set_num_tasks_isSet(true);
      }
      if (incoming.get(2)) {
        struct.emitted = iprot.readI64();
        struct.set_emitted_isSet(true);
      }
      if (incoming.get(3)) {
        struct.transferred = iprot.readI64();
        struct.set_transferred_isSet(true);
      }
      if (incoming.get(4)) {
        struct.acked = iprot.readI64();
        struct.set_acked_isSet(true);
      }
      if (incoming.get(5)) {
        struct.failed = iprot.readI64();
        struct.set_failed_isSet(true);
      }
      if (incoming.get(6)) {
        {
          org.apache.thrift.protocol.TMap _map360 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, iprot.readI32());
          struct.resources_map = new HashMap<String,Double>(2*_map360.size);
          String _key361;
          double _val362;
          for (int _i363 = 0; _i363 < _map360.size; ++_i363)
          {
            _key361 = iprot.readString();
            _val362 = iprot.readDouble();
            struct.resources_map.put(_key361, _val362);
          }
        }
        struct.set_resources_map_isSet(true);
      }
    }
  }

}

