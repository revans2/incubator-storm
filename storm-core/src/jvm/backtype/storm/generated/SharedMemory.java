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
public class SharedMemory implements org.apache.thrift.TBase<SharedMemory, SharedMemory._Fields>, java.io.Serializable, Cloneable, Comparable<SharedMemory> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SharedMemory");

  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField ON_HEAP_FIELD_DESC = new org.apache.thrift.protocol.TField("on_heap", org.apache.thrift.protocol.TType.DOUBLE, (short)2);
  private static final org.apache.thrift.protocol.TField OFF_HEAP_WORKER_FIELD_DESC = new org.apache.thrift.protocol.TField("off_heap_worker", org.apache.thrift.protocol.TType.DOUBLE, (short)3);
  private static final org.apache.thrift.protocol.TField OFF_HEAP_NODE_FIELD_DESC = new org.apache.thrift.protocol.TField("off_heap_node", org.apache.thrift.protocol.TType.DOUBLE, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SharedMemoryStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SharedMemoryTupleSchemeFactory());
  }

  private String name; // required
  private double on_heap; // optional
  private double off_heap_worker; // optional
  private double off_heap_node; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short)1, "name"),
    ON_HEAP((short)2, "on_heap"),
    OFF_HEAP_WORKER((short)3, "off_heap_worker"),
    OFF_HEAP_NODE((short)4, "off_heap_node");

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
        case 1: // NAME
          return NAME;
        case 2: // ON_HEAP
          return ON_HEAP;
        case 3: // OFF_HEAP_WORKER
          return OFF_HEAP_WORKER;
        case 4: // OFF_HEAP_NODE
          return OFF_HEAP_NODE;
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
  private static final int __ON_HEAP_ISSET_ID = 0;
  private static final int __OFF_HEAP_WORKER_ISSET_ID = 1;
  private static final int __OFF_HEAP_NODE_ISSET_ID = 2;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.ON_HEAP,_Fields.OFF_HEAP_WORKER,_Fields.OFF_HEAP_NODE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData("name", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ON_HEAP, new org.apache.thrift.meta_data.FieldMetaData("on_heap", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.OFF_HEAP_WORKER, new org.apache.thrift.meta_data.FieldMetaData("off_heap_worker", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.OFF_HEAP_NODE, new org.apache.thrift.meta_data.FieldMetaData("off_heap_node", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SharedMemory.class, metaDataMap);
  }

  public SharedMemory() {
  }

  public SharedMemory(
    String name)
  {
    this();
    this.name = name;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SharedMemory(SharedMemory other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.is_set_name()) {
      this.name = other.name;
    }
    this.on_heap = other.on_heap;
    this.off_heap_worker = other.off_heap_worker;
    this.off_heap_node = other.off_heap_node;
  }

  public SharedMemory deepCopy() {
    return new SharedMemory(this);
  }

  @Override
  public void clear() {
    this.name = null;
    set_on_heap_isSet(false);
    this.on_heap = 0.0;
    set_off_heap_worker_isSet(false);
    this.off_heap_worker = 0.0;
    set_off_heap_node_isSet(false);
    this.off_heap_node = 0.0;
  }

  public String get_name() {
    return this.name;
  }

  public void set_name(String name) {
    this.name = name;
  }

  public void unset_name() {
    this.name = null;
  }

  /** Returns true if field name is set (has been assigned a value) and false otherwise */
  public boolean is_set_name() {
    return this.name != null;
  }

  public void set_name_isSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public double get_on_heap() {
    return this.on_heap;
  }

  public void set_on_heap(double on_heap) {
    this.on_heap = on_heap;
    set_on_heap_isSet(true);
  }

  public void unset_on_heap() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __ON_HEAP_ISSET_ID);
  }

  /** Returns true if field on_heap is set (has been assigned a value) and false otherwise */
  public boolean is_set_on_heap() {
    return EncodingUtils.testBit(__isset_bitfield, __ON_HEAP_ISSET_ID);
  }

  public void set_on_heap_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __ON_HEAP_ISSET_ID, value);
  }

  public double get_off_heap_worker() {
    return this.off_heap_worker;
  }

  public void set_off_heap_worker(double off_heap_worker) {
    this.off_heap_worker = off_heap_worker;
    set_off_heap_worker_isSet(true);
  }

  public void unset_off_heap_worker() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __OFF_HEAP_WORKER_ISSET_ID);
  }

  /** Returns true if field off_heap_worker is set (has been assigned a value) and false otherwise */
  public boolean is_set_off_heap_worker() {
    return EncodingUtils.testBit(__isset_bitfield, __OFF_HEAP_WORKER_ISSET_ID);
  }

  public void set_off_heap_worker_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __OFF_HEAP_WORKER_ISSET_ID, value);
  }

  public double get_off_heap_node() {
    return this.off_heap_node;
  }

  public void set_off_heap_node(double off_heap_node) {
    this.off_heap_node = off_heap_node;
    set_off_heap_node_isSet(true);
  }

  public void unset_off_heap_node() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __OFF_HEAP_NODE_ISSET_ID);
  }

  /** Returns true if field off_heap_node is set (has been assigned a value) and false otherwise */
  public boolean is_set_off_heap_node() {
    return EncodingUtils.testBit(__isset_bitfield, __OFF_HEAP_NODE_ISSET_ID);
  }

  public void set_off_heap_node_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __OFF_HEAP_NODE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NAME:
      if (value == null) {
        unset_name();
      } else {
        set_name((String)value);
      }
      break;

    case ON_HEAP:
      if (value == null) {
        unset_on_heap();
      } else {
        set_on_heap((Double)value);
      }
      break;

    case OFF_HEAP_WORKER:
      if (value == null) {
        unset_off_heap_worker();
      } else {
        set_off_heap_worker((Double)value);
      }
      break;

    case OFF_HEAP_NODE:
      if (value == null) {
        unset_off_heap_node();
      } else {
        set_off_heap_node((Double)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NAME:
      return get_name();

    case ON_HEAP:
      return get_on_heap();

    case OFF_HEAP_WORKER:
      return get_off_heap_worker();

    case OFF_HEAP_NODE:
      return get_off_heap_node();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NAME:
      return is_set_name();
    case ON_HEAP:
      return is_set_on_heap();
    case OFF_HEAP_WORKER:
      return is_set_off_heap_worker();
    case OFF_HEAP_NODE:
      return is_set_off_heap_node();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SharedMemory)
      return this.equals((SharedMemory)that);
    return false;
  }

  public boolean equals(SharedMemory that) {
    if (that == null)
      return false;

    boolean this_present_name = true && this.is_set_name();
    boolean that_present_name = true && that.is_set_name();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_on_heap = true && this.is_set_on_heap();
    boolean that_present_on_heap = true && that.is_set_on_heap();
    if (this_present_on_heap || that_present_on_heap) {
      if (!(this_present_on_heap && that_present_on_heap))
        return false;
      if (this.on_heap != that.on_heap)
        return false;
    }

    boolean this_present_off_heap_worker = true && this.is_set_off_heap_worker();
    boolean that_present_off_heap_worker = true && that.is_set_off_heap_worker();
    if (this_present_off_heap_worker || that_present_off_heap_worker) {
      if (!(this_present_off_heap_worker && that_present_off_heap_worker))
        return false;
      if (this.off_heap_worker != that.off_heap_worker)
        return false;
    }

    boolean this_present_off_heap_node = true && this.is_set_off_heap_node();
    boolean that_present_off_heap_node = true && that.is_set_off_heap_node();
    if (this_present_off_heap_node || that_present_off_heap_node) {
      if (!(this_present_off_heap_node && that_present_off_heap_node))
        return false;
      if (this.off_heap_node != that.off_heap_node)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_name = true && (is_set_name());
    list.add(present_name);
    if (present_name)
      list.add(name);

    boolean present_on_heap = true && (is_set_on_heap());
    list.add(present_on_heap);
    if (present_on_heap)
      list.add(on_heap);

    boolean present_off_heap_worker = true && (is_set_off_heap_worker());
    list.add(present_off_heap_worker);
    if (present_off_heap_worker)
      list.add(off_heap_worker);

    boolean present_off_heap_node = true && (is_set_off_heap_node());
    list.add(present_off_heap_node);
    if (present_off_heap_node)
      list.add(off_heap_node);

    return list.hashCode();
  }

  @Override
  public int compareTo(SharedMemory other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_name()).compareTo(other.is_set_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.name, other.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_on_heap()).compareTo(other.is_set_on_heap());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_on_heap()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.on_heap, other.on_heap);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_off_heap_worker()).compareTo(other.is_set_off_heap_worker());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_off_heap_worker()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.off_heap_worker, other.off_heap_worker);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_off_heap_node()).compareTo(other.is_set_off_heap_node());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_off_heap_node()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.off_heap_node, other.off_heap_node);
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
    StringBuilder sb = new StringBuilder("SharedMemory(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (is_set_on_heap()) {
      if (!first) sb.append(", ");
      sb.append("on_heap:");
      sb.append(this.on_heap);
      first = false;
    }
    if (is_set_off_heap_worker()) {
      if (!first) sb.append(", ");
      sb.append("off_heap_worker:");
      sb.append(this.off_heap_worker);
      first = false;
    }
    if (is_set_off_heap_node()) {
      if (!first) sb.append(", ");
      sb.append("off_heap_node:");
      sb.append(this.off_heap_node);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_name()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'name' is unset! Struct:" + toString());
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SharedMemoryStandardSchemeFactory implements SchemeFactory {
    public SharedMemoryStandardScheme getScheme() {
      return new SharedMemoryStandardScheme();
    }
  }

  private static class SharedMemoryStandardScheme extends StandardScheme<SharedMemory> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SharedMemory struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.name = iprot.readString();
              struct.set_name_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ON_HEAP
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.on_heap = iprot.readDouble();
              struct.set_on_heap_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // OFF_HEAP_WORKER
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.off_heap_worker = iprot.readDouble();
              struct.set_off_heap_worker_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // OFF_HEAP_NODE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.off_heap_node = iprot.readDouble();
              struct.set_off_heap_node_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SharedMemory struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_on_heap()) {
        oprot.writeFieldBegin(ON_HEAP_FIELD_DESC);
        oprot.writeDouble(struct.on_heap);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_off_heap_worker()) {
        oprot.writeFieldBegin(OFF_HEAP_WORKER_FIELD_DESC);
        oprot.writeDouble(struct.off_heap_worker);
        oprot.writeFieldEnd();
      }
      if (struct.is_set_off_heap_node()) {
        oprot.writeFieldBegin(OFF_HEAP_NODE_FIELD_DESC);
        oprot.writeDouble(struct.off_heap_node);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SharedMemoryTupleSchemeFactory implements SchemeFactory {
    public SharedMemoryTupleScheme getScheme() {
      return new SharedMemoryTupleScheme();
    }
  }

  private static class SharedMemoryTupleScheme extends TupleScheme<SharedMemory> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SharedMemory struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.name);
      BitSet optionals = new BitSet();
      if (struct.is_set_on_heap()) {
        optionals.set(0);
      }
      if (struct.is_set_off_heap_worker()) {
        optionals.set(1);
      }
      if (struct.is_set_off_heap_node()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.is_set_on_heap()) {
        oprot.writeDouble(struct.on_heap);
      }
      if (struct.is_set_off_heap_worker()) {
        oprot.writeDouble(struct.off_heap_worker);
      }
      if (struct.is_set_off_heap_node()) {
        oprot.writeDouble(struct.off_heap_node);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SharedMemory struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.name = iprot.readString();
      struct.set_name_isSet(true);
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.on_heap = iprot.readDouble();
        struct.set_on_heap_isSet(true);
      }
      if (incoming.get(1)) {
        struct.off_heap_worker = iprot.readDouble();
        struct.set_off_heap_worker_isSet(true);
      }
      if (incoming.get(2)) {
        struct.off_heap_node = iprot.readDouble();
        struct.set_off_heap_node_isSet(true);
      }
    }
  }

}

