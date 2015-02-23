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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-2-23")
public class ReadableBlobMeta implements org.apache.thrift.TBase<ReadableBlobMeta, ReadableBlobMeta._Fields>, java.io.Serializable, Cloneable, Comparable<ReadableBlobMeta> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("ReadableBlobMeta");

  private static final org.apache.thrift.protocol.TField SETTABLE_FIELD_DESC = new org.apache.thrift.protocol.TField("settable", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField VERSION_FIELD_DESC = new org.apache.thrift.protocol.TField("version", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new ReadableBlobMetaStandardSchemeFactory());
    schemes.put(TupleScheme.class, new ReadableBlobMetaTupleSchemeFactory());
  }

  private SettableBlobMeta settable; // required
  private long version; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SETTABLE((short)1, "settable"),
    VERSION((short)2, "version");

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
        case 1: // SETTABLE
          return SETTABLE;
        case 2: // VERSION
          return VERSION;
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
  private static final int __VERSION_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SETTABLE, new org.apache.thrift.meta_data.FieldMetaData("settable", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SettableBlobMeta.class)));
    tmpMap.put(_Fields.VERSION, new org.apache.thrift.meta_data.FieldMetaData("version", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(ReadableBlobMeta.class, metaDataMap);
  }

  public ReadableBlobMeta() {
  }

  public ReadableBlobMeta(
    SettableBlobMeta settable,
    long version)
  {
    this();
    this.settable = settable;
    this.version = version;
    set_version_isSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ReadableBlobMeta(ReadableBlobMeta other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.is_set_settable()) {
      this.settable = new SettableBlobMeta(other.settable);
    }
    this.version = other.version;
  }

  public ReadableBlobMeta deepCopy() {
    return new ReadableBlobMeta(this);
  }

  @Override
  public void clear() {
    this.settable = null;
    set_version_isSet(false);
    this.version = 0;
  }

  public SettableBlobMeta get_settable() {
    return this.settable;
  }

  public void set_settable(SettableBlobMeta settable) {
    this.settable = settable;
  }

  public void unset_settable() {
    this.settable = null;
  }

  /** Returns true if field settable is set (has been assigned a value) and false otherwise */
  public boolean is_set_settable() {
    return this.settable != null;
  }

  public void set_settable_isSet(boolean value) {
    if (!value) {
      this.settable = null;
    }
  }

  public long get_version() {
    return this.version;
  }

  public void set_version(long version) {
    this.version = version;
    set_version_isSet(true);
  }

  public void unset_version() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __VERSION_ISSET_ID);
  }

  /** Returns true if field version is set (has been assigned a value) and false otherwise */
  public boolean is_set_version() {
    return EncodingUtils.testBit(__isset_bitfield, __VERSION_ISSET_ID);
  }

  public void set_version_isSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __VERSION_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SETTABLE:
      if (value == null) {
        unset_settable();
      } else {
        set_settable((SettableBlobMeta)value);
      }
      break;

    case VERSION:
      if (value == null) {
        unset_version();
      } else {
        set_version((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SETTABLE:
      return get_settable();

    case VERSION:
      return Long.valueOf(get_version());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SETTABLE:
      return is_set_settable();
    case VERSION:
      return is_set_version();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ReadableBlobMeta)
      return this.equals((ReadableBlobMeta)that);
    return false;
  }

  public boolean equals(ReadableBlobMeta that) {
    if (that == null)
      return false;

    boolean this_present_settable = true && this.is_set_settable();
    boolean that_present_settable = true && that.is_set_settable();
    if (this_present_settable || that_present_settable) {
      if (!(this_present_settable && that_present_settable))
        return false;
      if (!this.settable.equals(that.settable))
        return false;
    }

    boolean this_present_version = true;
    boolean that_present_version = true;
    if (this_present_version || that_present_version) {
      if (!(this_present_version && that_present_version))
        return false;
      if (this.version != that.version)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_settable = true && (is_set_settable());
    list.add(present_settable);
    if (present_settable)
      list.add(settable);

    boolean present_version = true;
    list.add(present_version);
    if (present_version)
      list.add(version);

    return list.hashCode();
  }

  @Override
  public int compareTo(ReadableBlobMeta other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_settable()).compareTo(other.is_set_settable());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_settable()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.settable, other.settable);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_version()).compareTo(other.is_set_version());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_version()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.version, other.version);
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
    StringBuilder sb = new StringBuilder("ReadableBlobMeta(");
    boolean first = true;

    sb.append("settable:");
    if (this.settable == null) {
      sb.append("null");
    } else {
      sb.append(this.settable);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("version:");
    sb.append(this.version);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_settable()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'settable' is unset! Struct:" + toString());
    }

    if (!is_set_version()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'version' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (settable != null) {
      settable.validate();
    }
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

  private static class ReadableBlobMetaStandardSchemeFactory implements SchemeFactory {
    public ReadableBlobMetaStandardScheme getScheme() {
      return new ReadableBlobMetaStandardScheme();
    }
  }

  private static class ReadableBlobMetaStandardScheme extends StandardScheme<ReadableBlobMeta> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, ReadableBlobMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SETTABLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.settable = new SettableBlobMeta();
              struct.settable.read(iprot);
              struct.set_settable_isSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // VERSION
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.version = iprot.readI64();
              struct.set_version_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, ReadableBlobMeta struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.settable != null) {
        oprot.writeFieldBegin(SETTABLE_FIELD_DESC);
        struct.settable.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(VERSION_FIELD_DESC);
      oprot.writeI64(struct.version);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class ReadableBlobMetaTupleSchemeFactory implements SchemeFactory {
    public ReadableBlobMetaTupleScheme getScheme() {
      return new ReadableBlobMetaTupleScheme();
    }
  }

  private static class ReadableBlobMetaTupleScheme extends TupleScheme<ReadableBlobMeta> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, ReadableBlobMeta struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.settable.write(oprot);
      oprot.writeI64(struct.version);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, ReadableBlobMeta struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.settable = new SettableBlobMeta();
      struct.settable.read(iprot);
      struct.set_settable_isSet(true);
      struct.version = iprot.readI64();
      struct.set_version_isSet(true);
    }
  }

}

