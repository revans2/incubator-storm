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
public class StormWindowedStats implements org.apache.thrift.TBase<StormWindowedStats, StormWindowedStats._Fields>, java.io.Serializable, Cloneable, Comparable<StormWindowedStats> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("StormWindowedStats");

  private static final org.apache.thrift.protocol.TField WINDOW_FIELD_DESC = new org.apache.thrift.protocol.TField("window", org.apache.thrift.protocol.TType.I32, (short)1);
  private static final org.apache.thrift.protocol.TField TOPOLOGY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("topology_id", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField COMPONENT_FIELD_DESC = new org.apache.thrift.protocol.TField("component", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField EXECUTOR_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("executor_id", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField("values", org.apache.thrift.protocol.TType.MAP, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new StormWindowedStatsStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StormWindowedStatsTupleSchemeFactory());
  }

  private Window window; // optional
  private String topology_id; // optional
  private String component; // optional
  private String executor_id; // optional
  private Map<String,Double> values; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * 
     * @see Window
     */
    WINDOW((short)1, "window"),
    TOPOLOGY_ID((short)2, "topology_id"),
    COMPONENT((short)3, "component"),
    EXECUTOR_ID((short)4, "executor_id"),
    VALUES((short)5, "values");

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
        case 1: // WINDOW
          return WINDOW;
        case 2: // TOPOLOGY_ID
          return TOPOLOGY_ID;
        case 3: // COMPONENT
          return COMPONENT;
        case 4: // EXECUTOR_ID
          return EXECUTOR_ID;
        case 5: // VALUES
          return VALUES;
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
  private static final _Fields optionals[] = {_Fields.WINDOW,_Fields.TOPOLOGY_ID,_Fields.COMPONENT,_Fields.EXECUTOR_ID,_Fields.VALUES};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.WINDOW, new org.apache.thrift.meta_data.FieldMetaData("window", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, Window.class)));
    tmpMap.put(_Fields.TOPOLOGY_ID, new org.apache.thrift.meta_data.FieldMetaData("topology_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.COMPONENT, new org.apache.thrift.meta_data.FieldMetaData("component", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.EXECUTOR_ID, new org.apache.thrift.meta_data.FieldMetaData("executor_id", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.VALUES, new org.apache.thrift.meta_data.FieldMetaData("values", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(StormWindowedStats.class, metaDataMap);
  }

  public StormWindowedStats() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public StormWindowedStats(StormWindowedStats other) {
    if (other.is_set_window()) {
      this.window = other.window;
    }
    if (other.is_set_topology_id()) {
      this.topology_id = other.topology_id;
    }
    if (other.is_set_component()) {
      this.component = other.component;
    }
    if (other.is_set_executor_id()) {
      this.executor_id = other.executor_id;
    }
    if (other.is_set_values()) {
      Map<String,Double> __this__values = new HashMap<String,Double>(other.values);
      this.values = __this__values;
    }
  }

  public StormWindowedStats deepCopy() {
    return new StormWindowedStats(this);
  }

  @Override
  public void clear() {
    this.window = null;
    this.topology_id = null;
    this.component = null;
    this.executor_id = null;
    this.values = null;
  }

  /**
   * 
   * @see Window
   */
  public Window get_window() {
    return this.window;
  }

  /**
   * 
   * @see Window
   */
  public void set_window(Window window) {
    this.window = window;
  }

  public void unset_window() {
    this.window = null;
  }

  /** Returns true if field window is set (has been assigned a value) and false otherwise */
  public boolean is_set_window() {
    return this.window != null;
  }

  public void set_window_isSet(boolean value) {
    if (!value) {
      this.window = null;
    }
  }

  public String get_topology_id() {
    return this.topology_id;
  }

  public void set_topology_id(String topology_id) {
    this.topology_id = topology_id;
  }

  public void unset_topology_id() {
    this.topology_id = null;
  }

  /** Returns true if field topology_id is set (has been assigned a value) and false otherwise */
  public boolean is_set_topology_id() {
    return this.topology_id != null;
  }

  public void set_topology_id_isSet(boolean value) {
    if (!value) {
      this.topology_id = null;
    }
  }

  public String get_component() {
    return this.component;
  }

  public void set_component(String component) {
    this.component = component;
  }

  public void unset_component() {
    this.component = null;
  }

  /** Returns true if field component is set (has been assigned a value) and false otherwise */
  public boolean is_set_component() {
    return this.component != null;
  }

  public void set_component_isSet(boolean value) {
    if (!value) {
      this.component = null;
    }
  }

  public String get_executor_id() {
    return this.executor_id;
  }

  public void set_executor_id(String executor_id) {
    this.executor_id = executor_id;
  }

  public void unset_executor_id() {
    this.executor_id = null;
  }

  /** Returns true if field executor_id is set (has been assigned a value) and false otherwise */
  public boolean is_set_executor_id() {
    return this.executor_id != null;
  }

  public void set_executor_id_isSet(boolean value) {
    if (!value) {
      this.executor_id = null;
    }
  }

  public int get_values_size() {
    return (this.values == null) ? 0 : this.values.size();
  }

  public void put_to_values(String key, double val) {
    if (this.values == null) {
      this.values = new HashMap<String,Double>();
    }
    this.values.put(key, val);
  }

  public Map<String,Double> get_values() {
    return this.values;
  }

  public void set_values(Map<String,Double> values) {
    this.values = values;
  }

  public void unset_values() {
    this.values = null;
  }

  /** Returns true if field values is set (has been assigned a value) and false otherwise */
  public boolean is_set_values() {
    return this.values != null;
  }

  public void set_values_isSet(boolean value) {
    if (!value) {
      this.values = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case WINDOW:
      if (value == null) {
        unset_window();
      } else {
        set_window((Window)value);
      }
      break;

    case TOPOLOGY_ID:
      if (value == null) {
        unset_topology_id();
      } else {
        set_topology_id((String)value);
      }
      break;

    case COMPONENT:
      if (value == null) {
        unset_component();
      } else {
        set_component((String)value);
      }
      break;

    case EXECUTOR_ID:
      if (value == null) {
        unset_executor_id();
      } else {
        set_executor_id((String)value);
      }
      break;

    case VALUES:
      if (value == null) {
        unset_values();
      } else {
        set_values((Map<String,Double>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case WINDOW:
      return get_window();

    case TOPOLOGY_ID:
      return get_topology_id();

    case COMPONENT:
      return get_component();

    case EXECUTOR_ID:
      return get_executor_id();

    case VALUES:
      return get_values();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case WINDOW:
      return is_set_window();
    case TOPOLOGY_ID:
      return is_set_topology_id();
    case COMPONENT:
      return is_set_component();
    case EXECUTOR_ID:
      return is_set_executor_id();
    case VALUES:
      return is_set_values();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof StormWindowedStats)
      return this.equals((StormWindowedStats)that);
    return false;
  }

  public boolean equals(StormWindowedStats that) {
    if (that == null)
      return false;

    boolean this_present_window = true && this.is_set_window();
    boolean that_present_window = true && that.is_set_window();
    if (this_present_window || that_present_window) {
      if (!(this_present_window && that_present_window))
        return false;
      if (!this.window.equals(that.window))
        return false;
    }

    boolean this_present_topology_id = true && this.is_set_topology_id();
    boolean that_present_topology_id = true && that.is_set_topology_id();
    if (this_present_topology_id || that_present_topology_id) {
      if (!(this_present_topology_id && that_present_topology_id))
        return false;
      if (!this.topology_id.equals(that.topology_id))
        return false;
    }

    boolean this_present_component = true && this.is_set_component();
    boolean that_present_component = true && that.is_set_component();
    if (this_present_component || that_present_component) {
      if (!(this_present_component && that_present_component))
        return false;
      if (!this.component.equals(that.component))
        return false;
    }

    boolean this_present_executor_id = true && this.is_set_executor_id();
    boolean that_present_executor_id = true && that.is_set_executor_id();
    if (this_present_executor_id || that_present_executor_id) {
      if (!(this_present_executor_id && that_present_executor_id))
        return false;
      if (!this.executor_id.equals(that.executor_id))
        return false;
    }

    boolean this_present_values = true && this.is_set_values();
    boolean that_present_values = true && that.is_set_values();
    if (this_present_values || that_present_values) {
      if (!(this_present_values && that_present_values))
        return false;
      if (!this.values.equals(that.values))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_window = true && (is_set_window());
    list.add(present_window);
    if (present_window)
      list.add(window.getValue());

    boolean present_topology_id = true && (is_set_topology_id());
    list.add(present_topology_id);
    if (present_topology_id)
      list.add(topology_id);

    boolean present_component = true && (is_set_component());
    list.add(present_component);
    if (present_component)
      list.add(component);

    boolean present_executor_id = true && (is_set_executor_id());
    list.add(present_executor_id);
    if (present_executor_id)
      list.add(executor_id);

    boolean present_values = true && (is_set_values());
    list.add(present_values);
    if (present_values)
      list.add(values);

    return list.hashCode();
  }

  @Override
  public int compareTo(StormWindowedStats other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_window()).compareTo(other.is_set_window());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_window()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.window, other.window);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_topology_id()).compareTo(other.is_set_topology_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_topology_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topology_id, other.topology_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_component()).compareTo(other.is_set_component());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_component()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.component, other.component);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_executor_id()).compareTo(other.is_set_executor_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_executor_id()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.executor_id, other.executor_id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_values()).compareTo(other.is_set_values());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_values()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.values, other.values);
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
    StringBuilder sb = new StringBuilder("StormWindowedStats(");
    boolean first = true;

    if (is_set_window()) {
      sb.append("window:");
      if (this.window == null) {
        sb.append("null");
      } else {
        sb.append(this.window);
      }
      first = false;
    }
    if (is_set_topology_id()) {
      if (!first) sb.append(", ");
      sb.append("topology_id:");
      if (this.topology_id == null) {
        sb.append("null");
      } else {
        sb.append(this.topology_id);
      }
      first = false;
    }
    if (is_set_component()) {
      if (!first) sb.append(", ");
      sb.append("component:");
      if (this.component == null) {
        sb.append("null");
      } else {
        sb.append(this.component);
      }
      first = false;
    }
    if (is_set_executor_id()) {
      if (!first) sb.append(", ");
      sb.append("executor_id:");
      if (this.executor_id == null) {
        sb.append("null");
      } else {
        sb.append(this.executor_id);
      }
      first = false;
    }
    if (is_set_values()) {
      if (!first) sb.append(", ");
      sb.append("values:");
      if (this.values == null) {
        sb.append("null");
      } else {
        sb.append(this.values);
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

  private static class StormWindowedStatsStandardSchemeFactory implements SchemeFactory {
    public StormWindowedStatsStandardScheme getScheme() {
      return new StormWindowedStatsStandardScheme();
    }
  }

  private static class StormWindowedStatsStandardScheme extends StandardScheme<StormWindowedStats> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, StormWindowedStats struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // WINDOW
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.window = org.apache.storm.generated.Window.findByValue(iprot.readI32());
              struct.set_window_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TOPOLOGY_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.topology_id = iprot.readString();
              struct.set_topology_id_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // COMPONENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.component = iprot.readString();
              struct.set_component_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // EXECUTOR_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.executor_id = iprot.readString();
              struct.set_executor_id_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // VALUES
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map924 = iprot.readMapBegin();
                struct.values = new HashMap<String,Double>(2*_map924.size);
                String _key925;
                double _val926;
                for (int _i927 = 0; _i927 < _map924.size; ++_i927)
                {
                  _key925 = iprot.readString();
                  _val926 = iprot.readDouble();
                  struct.values.put(_key925, _val926);
                }
                iprot.readMapEnd();
              }
              struct.set_values_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, StormWindowedStats struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.window != null) {
        if (struct.is_set_window()) {
          oprot.writeFieldBegin(WINDOW_FIELD_DESC);
          oprot.writeI32(struct.window.getValue());
          oprot.writeFieldEnd();
        }
      }
      if (struct.topology_id != null) {
        if (struct.is_set_topology_id()) {
          oprot.writeFieldBegin(TOPOLOGY_ID_FIELD_DESC);
          oprot.writeString(struct.topology_id);
          oprot.writeFieldEnd();
        }
      }
      if (struct.component != null) {
        if (struct.is_set_component()) {
          oprot.writeFieldBegin(COMPONENT_FIELD_DESC);
          oprot.writeString(struct.component);
          oprot.writeFieldEnd();
        }
      }
      if (struct.executor_id != null) {
        if (struct.is_set_executor_id()) {
          oprot.writeFieldBegin(EXECUTOR_ID_FIELD_DESC);
          oprot.writeString(struct.executor_id);
          oprot.writeFieldEnd();
        }
      }
      if (struct.values != null) {
        if (struct.is_set_values()) {
          oprot.writeFieldBegin(VALUES_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, struct.values.size()));
            for (Map.Entry<String, Double> _iter928 : struct.values.entrySet())
            {
              oprot.writeString(_iter928.getKey());
              oprot.writeDouble(_iter928.getValue());
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

  private static class StormWindowedStatsTupleSchemeFactory implements SchemeFactory {
    public StormWindowedStatsTupleScheme getScheme() {
      return new StormWindowedStatsTupleScheme();
    }
  }

  private static class StormWindowedStatsTupleScheme extends TupleScheme<StormWindowedStats> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, StormWindowedStats struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.is_set_window()) {
        optionals.set(0);
      }
      if (struct.is_set_topology_id()) {
        optionals.set(1);
      }
      if (struct.is_set_component()) {
        optionals.set(2);
      }
      if (struct.is_set_executor_id()) {
        optionals.set(3);
      }
      if (struct.is_set_values()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.is_set_window()) {
        oprot.writeI32(struct.window.getValue());
      }
      if (struct.is_set_topology_id()) {
        oprot.writeString(struct.topology_id);
      }
      if (struct.is_set_component()) {
        oprot.writeString(struct.component);
      }
      if (struct.is_set_executor_id()) {
        oprot.writeString(struct.executor_id);
      }
      if (struct.is_set_values()) {
        {
          oprot.writeI32(struct.values.size());
          for (Map.Entry<String, Double> _iter929 : struct.values.entrySet())
          {
            oprot.writeString(_iter929.getKey());
            oprot.writeDouble(_iter929.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, StormWindowedStats struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.window = org.apache.storm.generated.Window.findByValue(iprot.readI32());
        struct.set_window_isSet(true);
      }
      if (incoming.get(1)) {
        struct.topology_id = iprot.readString();
        struct.set_topology_id_isSet(true);
      }
      if (incoming.get(2)) {
        struct.component = iprot.readString();
        struct.set_component_isSet(true);
      }
      if (incoming.get(3)) {
        struct.executor_id = iprot.readString();
        struct.set_executor_id_isSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TMap _map930 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.DOUBLE, iprot.readI32());
          struct.values = new HashMap<String,Double>(2*_map930.size);
          String _key931;
          double _val932;
          for (int _i933 = 0; _i933 < _map930.size; ++_i933)
          {
            _key931 = iprot.readString();
            _val932 = iprot.readDouble();
            struct.values.put(_key931, _val932);
          }
        }
        struct.set_values_isSet(true);
      }
    }
  }

}

