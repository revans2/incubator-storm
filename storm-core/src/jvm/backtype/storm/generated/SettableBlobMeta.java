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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-5-20")
public class SettableBlobMeta implements org.apache.thrift.TBase<SettableBlobMeta, SettableBlobMeta._Fields>, java.io.Serializable, Cloneable, Comparable<SettableBlobMeta> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SettableBlobMeta");

  private static final org.apache.thrift.protocol.TField ACL_FIELD_DESC = new org.apache.thrift.protocol.TField("acl", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SettableBlobMetaStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SettableBlobMetaTupleSchemeFactory());
  }

  private List<AccessControl> acl; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ACL((short)1, "acl");

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
        case 1: // ACL
          return ACL;
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
    tmpMap.put(_Fields.ACL, new org.apache.thrift.meta_data.FieldMetaData("acl", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, AccessControl.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SettableBlobMeta.class, metaDataMap);
  }

  public SettableBlobMeta() {
  }

  public SettableBlobMeta(
    List<AccessControl> acl)
  {
    this();
    this.acl = acl;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SettableBlobMeta(SettableBlobMeta other) {
    if (other.is_set_acl()) {
      List<AccessControl> __this__acl = new ArrayList<AccessControl>(other.acl.size());
      for (AccessControl other_element : other.acl) {
        __this__acl.add(new AccessControl(other_element));
      }
      this.acl = __this__acl;
    }
  }

  public SettableBlobMeta deepCopy() {
    return new SettableBlobMeta(this);
  }

  @Override
  public void clear() {
    this.acl = null;
  }

  public int get_acl_size() {
    return (this.acl == null) ? 0 : this.acl.size();
  }

  public java.util.Iterator<AccessControl> get_acl_iterator() {
    return (this.acl == null) ? null : this.acl.iterator();
  }

  public void add_to_acl(AccessControl elem) {
    if (this.acl == null) {
      this.acl = new ArrayList<AccessControl>();
    }
    this.acl.add(elem);
  }

  public List<AccessControl> get_acl() {
    return this.acl;
  }

  public void set_acl(List<AccessControl> acl) {
    this.acl = acl;
  }

  public void unset_acl() {
    this.acl = null;
  }

  /** Returns true if field acl is set (has been assigned a value) and false otherwise */
  public boolean is_set_acl() {
    return this.acl != null;
  }

  public void set_acl_isSet(boolean value) {
    if (!value) {
      this.acl = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ACL:
      if (value == null) {
        unset_acl();
      } else {
        set_acl((List<AccessControl>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ACL:
      return get_acl();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ACL:
      return is_set_acl();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SettableBlobMeta)
      return this.equals((SettableBlobMeta)that);
    return false;
  }

  public boolean equals(SettableBlobMeta that) {
    if (that == null)
      return false;

    boolean this_present_acl = true && this.is_set_acl();
    boolean that_present_acl = true && that.is_set_acl();
    if (this_present_acl || that_present_acl) {
      if (!(this_present_acl && that_present_acl))
        return false;
      if (!this.acl.equals(that.acl))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_acl = true && (is_set_acl());
    list.add(present_acl);
    if (present_acl)
      list.add(acl);

    return list.hashCode();
  }

  @Override
  public int compareTo(SettableBlobMeta other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_acl()).compareTo(other.is_set_acl());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_acl()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.acl, other.acl);
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
    StringBuilder sb = new StringBuilder("SettableBlobMeta(");
    boolean first = true;

    sb.append("acl:");
    if (this.acl == null) {
      sb.append("null");
    } else {
      sb.append(this.acl);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_acl()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'acl' is unset! Struct:" + toString());
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

  private static class SettableBlobMetaStandardSchemeFactory implements SchemeFactory {
    public SettableBlobMetaStandardScheme getScheme() {
      return new SettableBlobMetaStandardScheme();
    }
  }

  private static class SettableBlobMetaStandardScheme extends StandardScheme<SettableBlobMeta> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, SettableBlobMeta struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ACL
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list452 = iprot.readListBegin();
                struct.acl = new ArrayList<AccessControl>(_list452.size);
                AccessControl _elem453;
                for (int _i454 = 0; _i454 < _list452.size; ++_i454)
                {
                  _elem453 = new AccessControl();
                  _elem453.read(iprot);
                  struct.acl.add(_elem453);
                }
                iprot.readListEnd();
              }
              struct.set_acl_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, SettableBlobMeta struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.acl != null) {
        oprot.writeFieldBegin(ACL_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.acl.size()));
          for (AccessControl _iter455 : struct.acl)
          {
            _iter455.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SettableBlobMetaTupleSchemeFactory implements SchemeFactory {
    public SettableBlobMetaTupleScheme getScheme() {
      return new SettableBlobMetaTupleScheme();
    }
  }

  private static class SettableBlobMetaTupleScheme extends TupleScheme<SettableBlobMeta> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, SettableBlobMeta struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.acl.size());
        for (AccessControl _iter456 : struct.acl)
        {
          _iter456.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SettableBlobMeta struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list457 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.acl = new ArrayList<AccessControl>(_list457.size);
        AccessControl _elem458;
        for (int _i459 = 0; _i459 < _list457.size; ++_i459)
        {
          _elem458 = new AccessControl();
          _elem458.read(iprot);
          struct.acl.add(_elem458);
        }
      }
      struct.set_acl_isSet(true);
    }
  }

}

