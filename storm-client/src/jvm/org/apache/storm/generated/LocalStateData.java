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
 * Autogenerated by Thrift Compiler (0.11.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.storm.generated;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.11.0)")
public class LocalStateData implements org.apache.thrift.TBase<LocalStateData, LocalStateData._Fields>, java.io.Serializable, Cloneable, Comparable<LocalStateData> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LocalStateData");

  private static final org.apache.thrift.protocol.TField SERIALIZED_PARTS_FIELD_DESC = new org.apache.thrift.protocol.TField("serialized_parts", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new LocalStateDataStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new LocalStateDataTupleSchemeFactory();

  private java.util.Map<java.lang.String,ThriftSerializedObject> serialized_parts; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SERIALIZED_PARTS((short)1, "serialized_parts");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SERIALIZED_PARTS
          return SERIALIZED_PARTS;
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
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SERIALIZED_PARTS, new org.apache.thrift.meta_data.FieldMetaData("serialized_parts", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ThriftSerializedObject.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LocalStateData.class, metaDataMap);
  }

  public LocalStateData() {
  }

  public LocalStateData(
    java.util.Map<java.lang.String,ThriftSerializedObject> serialized_parts)
  {
    this();
    this.serialized_parts = serialized_parts;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LocalStateData(LocalStateData other) {
    if (other.is_set_serialized_parts()) {
      java.util.Map<java.lang.String,ThriftSerializedObject> __this__serialized_parts = new java.util.HashMap<java.lang.String,ThriftSerializedObject>(other.serialized_parts.size());
      for (java.util.Map.Entry<java.lang.String, ThriftSerializedObject> other_element : other.serialized_parts.entrySet()) {

        java.lang.String other_element_key = other_element.getKey();
        ThriftSerializedObject other_element_value = other_element.getValue();

        java.lang.String __this__serialized_parts_copy_key = other_element_key;

        ThriftSerializedObject __this__serialized_parts_copy_value = new ThriftSerializedObject(other_element_value);

        __this__serialized_parts.put(__this__serialized_parts_copy_key, __this__serialized_parts_copy_value);
      }
      this.serialized_parts = __this__serialized_parts;
    }
  }

  public LocalStateData deepCopy() {
    return new LocalStateData(this);
  }

  @Override
  public void clear() {
    this.serialized_parts = null;
  }

  public int get_serialized_parts_size() {
    return (this.serialized_parts == null) ? 0 : this.serialized_parts.size();
  }

  public void put_to_serialized_parts(java.lang.String key, ThriftSerializedObject val) {
    if (this.serialized_parts == null) {
      this.serialized_parts = new java.util.HashMap<java.lang.String,ThriftSerializedObject>();
    }
    this.serialized_parts.put(key, val);
  }

  public java.util.Map<java.lang.String,ThriftSerializedObject> get_serialized_parts() {
    return this.serialized_parts;
  }

  public void set_serialized_parts(java.util.Map<java.lang.String,ThriftSerializedObject> serialized_parts) {
    this.serialized_parts = serialized_parts;
  }

  public void unset_serialized_parts() {
    this.serialized_parts = null;
  }

  /** Returns true if field serialized_parts is set (has been assigned a value) and false otherwise */
  public boolean is_set_serialized_parts() {
    return this.serialized_parts != null;
  }

  public void set_serialized_parts_isSet(boolean value) {
    if (!value) {
      this.serialized_parts = null;
    }
  }

  public void setFieldValue(_Fields field, java.lang.Object value) {
    switch (field) {
    case SERIALIZED_PARTS:
      if (value == null) {
        unset_serialized_parts();
      } else {
        set_serialized_parts((java.util.Map<java.lang.String,ThriftSerializedObject>)value);
      }
      break;

    }
  }

  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case SERIALIZED_PARTS:
      return get_serialized_parts();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case SERIALIZED_PARTS:
      return is_set_serialized_parts();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof LocalStateData)
      return this.equals((LocalStateData)that);
    return false;
  }

  public boolean equals(LocalStateData that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_serialized_parts = true && this.is_set_serialized_parts();
    boolean that_present_serialized_parts = true && that.is_set_serialized_parts();
    if (this_present_serialized_parts || that_present_serialized_parts) {
      if (!(this_present_serialized_parts && that_present_serialized_parts))
        return false;
      if (!this.serialized_parts.equals(that.serialized_parts))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((is_set_serialized_parts()) ? 131071 : 524287);
    if (is_set_serialized_parts())
      hashCode = hashCode * 8191 + serialized_parts.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(LocalStateData other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(is_set_serialized_parts()).compareTo(other.is_set_serialized_parts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_serialized_parts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serialized_parts, other.serialized_parts);
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
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("LocalStateData(");
    boolean first = true;

    sb.append("serialized_parts:");
    if (this.serialized_parts == null) {
      sb.append("null");
    } else {
      sb.append(this.serialized_parts);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_serialized_parts()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'serialized_parts' is unset! Struct:" + toString());
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

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class LocalStateDataStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public LocalStateDataStandardScheme getScheme() {
      return new LocalStateDataStandardScheme();
    }
  }

  private static class LocalStateDataStandardScheme extends org.apache.thrift.scheme.StandardScheme<LocalStateData> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LocalStateData struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SERIALIZED_PARTS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map782 = iprot.readMapBegin();
                struct.serialized_parts = new java.util.HashMap<java.lang.String,ThriftSerializedObject>(2*_map782.size);
                java.lang.String _key783;
                ThriftSerializedObject _val784;
                for (int _i785 = 0; _i785 < _map782.size; ++_i785)
                {
                  _key783 = iprot.readString();
                  _val784 = new ThriftSerializedObject();
                  _val784.read(iprot);
                  struct.serialized_parts.put(_key783, _val784);
                }
                iprot.readMapEnd();
              }
              struct.set_serialized_parts_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LocalStateData struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.serialized_parts != null) {
        oprot.writeFieldBegin(SERIALIZED_PARTS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, struct.serialized_parts.size()));
          for (java.util.Map.Entry<java.lang.String, ThriftSerializedObject> _iter786 : struct.serialized_parts.entrySet())
          {
            oprot.writeString(_iter786.getKey());
            _iter786.getValue().write(oprot);
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LocalStateDataTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public LocalStateDataTupleScheme getScheme() {
      return new LocalStateDataTupleScheme();
    }
  }

  private static class LocalStateDataTupleScheme extends org.apache.thrift.scheme.TupleScheme<LocalStateData> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LocalStateData struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.serialized_parts.size());
        for (java.util.Map.Entry<java.lang.String, ThriftSerializedObject> _iter787 : struct.serialized_parts.entrySet())
        {
          oprot.writeString(_iter787.getKey());
          _iter787.getValue().write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LocalStateData struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map788 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.serialized_parts = new java.util.HashMap<java.lang.String,ThriftSerializedObject>(2*_map788.size);
        java.lang.String _key789;
        ThriftSerializedObject _val790;
        for (int _i791 = 0; _i791 < _map788.size; ++_i791)
        {
          _key789 = iprot.readString();
          _val790 = new ThriftSerializedObject();
          _val790.read(iprot);
          struct.serialized_parts.put(_key789, _val790);
        }
      }
      struct.set_serialized_parts_isSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

