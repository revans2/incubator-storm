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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-3-20")
public class StormTopology implements org.apache.thrift.TBase<StormTopology, StormTopology._Fields>, java.io.Serializable, Cloneable, Comparable<StormTopology> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("StormTopology");

  private static final org.apache.thrift.protocol.TField SPOUTS_FIELD_DESC = new org.apache.thrift.protocol.TField("spouts", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField BOLTS_FIELD_DESC = new org.apache.thrift.protocol.TField("bolts", org.apache.thrift.protocol.TType.MAP, (short)2);
  private static final org.apache.thrift.protocol.TField STATE_SPOUTS_FIELD_DESC = new org.apache.thrift.protocol.TField("state_spouts", org.apache.thrift.protocol.TType.MAP, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new StormTopologyStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StormTopologyTupleSchemeFactory());
  }

  private Map<String,SpoutSpec> spouts; // required
  private Map<String,Bolt> bolts; // required
  private Map<String,StateSpoutSpec> state_spouts; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SPOUTS((short)1, "spouts"),
    BOLTS((short)2, "bolts"),
    STATE_SPOUTS((short)3, "state_spouts");

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
        case 1: // SPOUTS
          return SPOUTS;
        case 2: // BOLTS
          return BOLTS;
        case 3: // STATE_SPOUTS
          return STATE_SPOUTS;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SPOUTS, new org.apache.thrift.meta_data.FieldMetaData("spouts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SpoutSpec.class))));
    tmpMap.put(_Fields.BOLTS, new org.apache.thrift.meta_data.FieldMetaData("bolts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Bolt.class))));
    tmpMap.put(_Fields.STATE_SPOUTS, new org.apache.thrift.meta_data.FieldMetaData("state_spouts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, StateSpoutSpec.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(StormTopology.class, metaDataMap);
  }

  public StormTopology() {
  }

  public StormTopology(
    Map<String,SpoutSpec> spouts,
    Map<String,Bolt> bolts,
    Map<String,StateSpoutSpec> state_spouts)
  {
    this();
    this.spouts = spouts;
    this.bolts = bolts;
    this.state_spouts = state_spouts;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public StormTopology(StormTopology other) {
    if (other.is_set_spouts()) {
      Map<String,SpoutSpec> __this__spouts = new HashMap<String,SpoutSpec>(other.spouts.size());
      for (Map.Entry<String, SpoutSpec> other_element : other.spouts.entrySet()) {

        String other_element_key = other_element.getKey();
        SpoutSpec other_element_value = other_element.getValue();

        String __this__spouts_copy_key = other_element_key;

        SpoutSpec __this__spouts_copy_value = new SpoutSpec(other_element_value);

        __this__spouts.put(__this__spouts_copy_key, __this__spouts_copy_value);
      }
      this.spouts = __this__spouts;
    }
    if (other.is_set_bolts()) {
      Map<String,Bolt> __this__bolts = new HashMap<String,Bolt>(other.bolts.size());
      for (Map.Entry<String, Bolt> other_element : other.bolts.entrySet()) {

        String other_element_key = other_element.getKey();
        Bolt other_element_value = other_element.getValue();

        String __this__bolts_copy_key = other_element_key;

        Bolt __this__bolts_copy_value = new Bolt(other_element_value);

        __this__bolts.put(__this__bolts_copy_key, __this__bolts_copy_value);
      }
      this.bolts = __this__bolts;
    }
    if (other.is_set_state_spouts()) {
      Map<String,StateSpoutSpec> __this__state_spouts = new HashMap<String,StateSpoutSpec>(other.state_spouts.size());
      for (Map.Entry<String, StateSpoutSpec> other_element : other.state_spouts.entrySet()) {

        String other_element_key = other_element.getKey();
        StateSpoutSpec other_element_value = other_element.getValue();

        String __this__state_spouts_copy_key = other_element_key;

        StateSpoutSpec __this__state_spouts_copy_value = new StateSpoutSpec(other_element_value);

        __this__state_spouts.put(__this__state_spouts_copy_key, __this__state_spouts_copy_value);
      }
      this.state_spouts = __this__state_spouts;
    }
  }

  public StormTopology deepCopy() {
    return new StormTopology(this);
  }

  @Override
  public void clear() {
    this.spouts = null;
    this.bolts = null;
    this.state_spouts = null;
  }

  public int get_spouts_size() {
    return (this.spouts == null) ? 0 : this.spouts.size();
  }

  public void put_to_spouts(String key, SpoutSpec val) {
    if (this.spouts == null) {
      this.spouts = new HashMap<String,SpoutSpec>();
    }
    this.spouts.put(key, val);
  }

  public Map<String,SpoutSpec> get_spouts() {
    return this.spouts;
  }

  public void set_spouts(Map<String,SpoutSpec> spouts) {
    this.spouts = spouts;
  }

  public void unset_spouts() {
    this.spouts = null;
  }

  /** Returns true if field spouts is set (has been assigned a value) and false otherwise */
  public boolean is_set_spouts() {
    return this.spouts != null;
  }

  public void set_spouts_isSet(boolean value) {
    if (!value) {
      this.spouts = null;
    }
  }

  public int get_bolts_size() {
    return (this.bolts == null) ? 0 : this.bolts.size();
  }

  public void put_to_bolts(String key, Bolt val) {
    if (this.bolts == null) {
      this.bolts = new HashMap<String,Bolt>();
    }
    this.bolts.put(key, val);
  }

  public Map<String,Bolt> get_bolts() {
    return this.bolts;
  }

  public void set_bolts(Map<String,Bolt> bolts) {
    this.bolts = bolts;
  }

  public void unset_bolts() {
    this.bolts = null;
  }

  /** Returns true if field bolts is set (has been assigned a value) and false otherwise */
  public boolean is_set_bolts() {
    return this.bolts != null;
  }

  public void set_bolts_isSet(boolean value) {
    if (!value) {
      this.bolts = null;
    }
  }

  public int get_state_spouts_size() {
    return (this.state_spouts == null) ? 0 : this.state_spouts.size();
  }

  public void put_to_state_spouts(String key, StateSpoutSpec val) {
    if (this.state_spouts == null) {
      this.state_spouts = new HashMap<String,StateSpoutSpec>();
    }
    this.state_spouts.put(key, val);
  }

  public Map<String,StateSpoutSpec> get_state_spouts() {
    return this.state_spouts;
  }

  public void set_state_spouts(Map<String,StateSpoutSpec> state_spouts) {
    this.state_spouts = state_spouts;
  }

  public void unset_state_spouts() {
    this.state_spouts = null;
  }

  /** Returns true if field state_spouts is set (has been assigned a value) and false otherwise */
  public boolean is_set_state_spouts() {
    return this.state_spouts != null;
  }

  public void set_state_spouts_isSet(boolean value) {
    if (!value) {
      this.state_spouts = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SPOUTS:
      if (value == null) {
        unset_spouts();
      } else {
        set_spouts((Map<String,SpoutSpec>)value);
      }
      break;

    case BOLTS:
      if (value == null) {
        unset_bolts();
      } else {
        set_bolts((Map<String,Bolt>)value);
      }
      break;

    case STATE_SPOUTS:
      if (value == null) {
        unset_state_spouts();
      } else {
        set_state_spouts((Map<String,StateSpoutSpec>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SPOUTS:
      return get_spouts();

    case BOLTS:
      return get_bolts();

    case STATE_SPOUTS:
      return get_state_spouts();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SPOUTS:
      return is_set_spouts();
    case BOLTS:
      return is_set_bolts();
    case STATE_SPOUTS:
      return is_set_state_spouts();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof StormTopology)
      return this.equals((StormTopology)that);
    return false;
  }

  public boolean equals(StormTopology that) {
    if (that == null)
      return false;

    boolean this_present_spouts = true && this.is_set_spouts();
    boolean that_present_spouts = true && that.is_set_spouts();
    if (this_present_spouts || that_present_spouts) {
      if (!(this_present_spouts && that_present_spouts))
        return false;
      if (!this.spouts.equals(that.spouts))
        return false;
    }

    boolean this_present_bolts = true && this.is_set_bolts();
    boolean that_present_bolts = true && that.is_set_bolts();
    if (this_present_bolts || that_present_bolts) {
      if (!(this_present_bolts && that_present_bolts))
        return false;
      if (!this.bolts.equals(that.bolts))
        return false;
    }

    boolean this_present_state_spouts = true && this.is_set_state_spouts();
    boolean that_present_state_spouts = true && that.is_set_state_spouts();
    if (this_present_state_spouts || that_present_state_spouts) {
      if (!(this_present_state_spouts && that_present_state_spouts))
        return false;
      if (!this.state_spouts.equals(that.state_spouts))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_spouts = true && (is_set_spouts());
    list.add(present_spouts);
    if (present_spouts)
      list.add(spouts);

    boolean present_bolts = true && (is_set_bolts());
    list.add(present_bolts);
    if (present_bolts)
      list.add(bolts);

    boolean present_state_spouts = true && (is_set_state_spouts());
    list.add(present_state_spouts);
    if (present_state_spouts)
      list.add(state_spouts);

    return list.hashCode();
  }

  @Override
  public int compareTo(StormTopology other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_spouts()).compareTo(other.is_set_spouts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_spouts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.spouts, other.spouts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_bolts()).compareTo(other.is_set_bolts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_bolts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.bolts, other.bolts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_state_spouts()).compareTo(other.is_set_state_spouts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_state_spouts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.state_spouts, other.state_spouts);
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
    StringBuilder sb = new StringBuilder("StormTopology(");
    boolean first = true;

    sb.append("spouts:");
    if (this.spouts == null) {
      sb.append("null");
    } else {
      sb.append(this.spouts);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("bolts:");
    if (this.bolts == null) {
      sb.append("null");
    } else {
      sb.append(this.bolts);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("state_spouts:");
    if (this.state_spouts == null) {
      sb.append("null");
    } else {
      sb.append(this.state_spouts);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_spouts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'spouts' is unset! Struct:" + toString());
    }

    if (!is_set_bolts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'bolts' is unset! Struct:" + toString());
    }

    if (!is_set_state_spouts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'state_spouts' is unset! Struct:" + toString());
    }

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

  private static class StormTopologyStandardSchemeFactory implements SchemeFactory {
    public StormTopologyStandardScheme getScheme() {
      return new StormTopologyStandardScheme();
    }
  }

  private static class StormTopologyStandardScheme extends StandardScheme<StormTopology> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, StormTopology struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SPOUTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map44 = iprot.readMapBegin();
                struct.spouts = new HashMap<String,SpoutSpec>(2*_map44.size);
                String _key45;
                SpoutSpec _val46;
                for (int _i47 = 0; _i47 < _map44.size; ++_i47)
                {
                  _key45 = iprot.readString();
                  _val46 = new SpoutSpec();
                  _val46.read(iprot);
                  struct.spouts.put(_key45, _val46);
                }
                iprot.readMapEnd();
              }
              struct.set_spouts_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // BOLTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map48 = iprot.readMapBegin();
                struct.bolts = new HashMap<String,Bolt>(2*_map48.size);
                String _key49;
                Bolt _val50;
                for (int _i51 = 0; _i51 < _map48.size; ++_i51)
                {
                  _key49 = iprot.readString();
                  _val50 = new Bolt();
                  _val50.read(iprot);
                  struct.bolts.put(_key49, _val50);
                }
                iprot.readMapEnd();
              }
              struct.set_bolts_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // STATE_SPOUTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map52 = iprot.readMapBegin();
                struct.state_spouts = new HashMap<String,StateSpoutSpec>(2*_map52.size);
                String _key53;
                StateSpoutSpec _val54;
                for (int _i55 = 0; _i55 < _map52.size; ++_i55)
                {
                  _key53 = iprot.readString();
                  _val54 = new StateSpoutSpec();
                  _val54.read(iprot);
                  struct.state_spouts.put(_key53, _val54);
                }
                iprot.readMapEnd();
              }
              struct.set_state_spouts_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, StormTopology struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.spouts != null) {
        oprot.writeFieldBegin(SPOUTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.spouts.size()));
          for (Map.Entry<String, SpoutSpec> _iter56 : struct.spouts.entrySet())
          {
            oprot.writeString(_iter56.getKey());
            _iter56.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.bolts != null) {
        oprot.writeFieldBegin(BOLTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.bolts.size()));
          for (Map.Entry<String, Bolt> _iter57 : struct.bolts.entrySet())
          {
            oprot.writeString(_iter57.getKey());
            _iter57.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.state_spouts != null) {
        oprot.writeFieldBegin(STATE_SPOUTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.state_spouts.size()));
          for (Map.Entry<String, StateSpoutSpec> _iter58 : struct.state_spouts.entrySet())
          {
            oprot.writeString(_iter58.getKey());
            _iter58.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class StormTopologyTupleSchemeFactory implements SchemeFactory {
    public StormTopologyTupleScheme getScheme() {
      return new StormTopologyTupleScheme();
    }
  }

  private static class StormTopologyTupleScheme extends TupleScheme<StormTopology> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, StormTopology struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.spouts.size());
        for (Map.Entry<String, SpoutSpec> _iter59 : struct.spouts.entrySet())
        {
          oprot.writeString(_iter59.getKey());
          _iter59.getValue().write(oprot);
        }
      }
      {
        oprot.writeI32(struct.bolts.size());
        for (Map.Entry<String, Bolt> _iter60 : struct.bolts.entrySet())
        {
          oprot.writeString(_iter60.getKey());
          _iter60.getValue().write(oprot);
        }
      }
      {
        oprot.writeI32(struct.state_spouts.size());
        for (Map.Entry<String, StateSpoutSpec> _iter61 : struct.state_spouts.entrySet())
        {
          oprot.writeString(_iter61.getKey());
          _iter61.getValue().write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, StormTopology struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map62 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.spouts = new HashMap<String,SpoutSpec>(2*_map62.size);
        String _key63;
        SpoutSpec _val64;
        for (int _i65 = 0; _i65 < _map62.size; ++_i65)
        {
          _key63 = iprot.readString();
          _val64 = new SpoutSpec();
          _val64.read(iprot);
          struct.spouts.put(_key63, _val64);
        }
      }
      struct.set_spouts_isSet(true);
      {
        org.apache.thrift.protocol.TMap _map66 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.bolts = new HashMap<String,Bolt>(2*_map66.size);
        String _key67;
        Bolt _val68;
        for (int _i69 = 0; _i69 < _map66.size; ++_i69)
        {
          _key67 = iprot.readString();
          _val68 = new Bolt();
          _val68.read(iprot);
          struct.bolts.put(_key67, _val68);
        }
      }
      struct.set_bolts_isSet(true);
      {
        org.apache.thrift.protocol.TMap _map70 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.state_spouts = new HashMap<String,StateSpoutSpec>(2*_map70.size);
        String _key71;
        StateSpoutSpec _val72;
        for (int _i73 = 0; _i73 < _map70.size; ++_i73)
        {
          _key71 = iprot.readString();
          _val72 = new StateSpoutSpec();
          _val72.read(iprot);
          struct.state_spouts.put(_key71, _val72);
        }
      }
      struct.set_state_spouts_isSet(true);
    }
  }

}

