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
public class LSApprovedWorkers implements org.apache.thrift.TBase<LSApprovedWorkers, LSApprovedWorkers._Fields>, java.io.Serializable, Cloneable, Comparable<LSApprovedWorkers> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LSApprovedWorkers");

  private static final org.apache.thrift.protocol.TField APPROVED_WORKERS_FIELD_DESC = new org.apache.thrift.protocol.TField("approved_workers", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LSApprovedWorkersStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LSApprovedWorkersTupleSchemeFactory());
  }

  private Map<String,Integer> approved_workers; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    APPROVED_WORKERS((short)1, "approved_workers");

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
        case 1: // APPROVED_WORKERS
          return APPROVED_WORKERS;
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
    tmpMap.put(_Fields.APPROVED_WORKERS, new org.apache.thrift.meta_data.FieldMetaData("approved_workers", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LSApprovedWorkers.class, metaDataMap);
  }

  public LSApprovedWorkers() {
  }

  public LSApprovedWorkers(
    Map<String,Integer> approved_workers)
  {
    this();
    this.approved_workers = approved_workers;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LSApprovedWorkers(LSApprovedWorkers other) {
    if (other.is_set_approved_workers()) {
      Map<String,Integer> __this__approved_workers = new HashMap<String,Integer>(other.approved_workers);
      this.approved_workers = __this__approved_workers;
    }
  }

  public LSApprovedWorkers deepCopy() {
    return new LSApprovedWorkers(this);
  }

  @Override
  public void clear() {
    this.approved_workers = null;
  }

  public int get_approved_workers_size() {
    return (this.approved_workers == null) ? 0 : this.approved_workers.size();
  }

  public void put_to_approved_workers(String key, int val) {
    if (this.approved_workers == null) {
      this.approved_workers = new HashMap<String,Integer>();
    }
    this.approved_workers.put(key, val);
  }

  public Map<String,Integer> get_approved_workers() {
    return this.approved_workers;
  }

  public void set_approved_workers(Map<String,Integer> approved_workers) {
    this.approved_workers = approved_workers;
  }

  public void unset_approved_workers() {
    this.approved_workers = null;
  }

  /** Returns true if field approved_workers is set (has been assigned a value) and false otherwise */
  public boolean is_set_approved_workers() {
    return this.approved_workers != null;
  }

  public void set_approved_workers_isSet(boolean value) {
    if (!value) {
      this.approved_workers = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case APPROVED_WORKERS:
      if (value == null) {
        unset_approved_workers();
      } else {
        set_approved_workers((Map<String,Integer>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case APPROVED_WORKERS:
      return get_approved_workers();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case APPROVED_WORKERS:
      return is_set_approved_workers();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LSApprovedWorkers)
      return this.equals((LSApprovedWorkers)that);
    return false;
  }

  public boolean equals(LSApprovedWorkers that) {
    if (that == null)
      return false;

    boolean this_present_approved_workers = true && this.is_set_approved_workers();
    boolean that_present_approved_workers = true && that.is_set_approved_workers();
    if (this_present_approved_workers || that_present_approved_workers) {
      if (!(this_present_approved_workers && that_present_approved_workers))
        return false;
      if (!this.approved_workers.equals(that.approved_workers))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_approved_workers = true && (is_set_approved_workers());
    list.add(present_approved_workers);
    if (present_approved_workers)
      list.add(approved_workers);

    return list.hashCode();
  }

  @Override
  public int compareTo(LSApprovedWorkers other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_approved_workers()).compareTo(other.is_set_approved_workers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_approved_workers()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.approved_workers, other.approved_workers);
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
    StringBuilder sb = new StringBuilder("LSApprovedWorkers(");
    boolean first = true;

    sb.append("approved_workers:");
    if (this.approved_workers == null) {
      sb.append("null");
    } else {
      sb.append(this.approved_workers);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_approved_workers()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'approved_workers' is unset! Struct:" + toString());
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

  private static class LSApprovedWorkersStandardSchemeFactory implements SchemeFactory {
    public LSApprovedWorkersStandardScheme getScheme() {
      return new LSApprovedWorkersStandardScheme();
    }
  }

  private static class LSApprovedWorkersStandardScheme extends StandardScheme<LSApprovedWorkers> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LSApprovedWorkers struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // APPROVED_WORKERS
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map754 = iprot.readMapBegin();
                struct.approved_workers = new HashMap<String,Integer>(2*_map754.size);
                String _key755;
                int _val756;
                for (int _i757 = 0; _i757 < _map754.size; ++_i757)
                {
                  _key755 = iprot.readString();
                  _val756 = iprot.readI32();
                  struct.approved_workers.put(_key755, _val756);
                }
                iprot.readMapEnd();
              }
              struct.set_approved_workers_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LSApprovedWorkers struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.approved_workers != null) {
        oprot.writeFieldBegin(APPROVED_WORKERS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I32, struct.approved_workers.size()));
          for (Map.Entry<String, Integer> _iter758 : struct.approved_workers.entrySet())
          {
            oprot.writeString(_iter758.getKey());
            oprot.writeI32(_iter758.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LSApprovedWorkersTupleSchemeFactory implements SchemeFactory {
    public LSApprovedWorkersTupleScheme getScheme() {
      return new LSApprovedWorkersTupleScheme();
    }
  }

  private static class LSApprovedWorkersTupleScheme extends TupleScheme<LSApprovedWorkers> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LSApprovedWorkers struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.approved_workers.size());
        for (Map.Entry<String, Integer> _iter759 : struct.approved_workers.entrySet())
        {
          oprot.writeString(_iter759.getKey());
          oprot.writeI32(_iter759.getValue());
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LSApprovedWorkers struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TMap _map760 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.I32, iprot.readI32());
        struct.approved_workers = new HashMap<String,Integer>(2*_map760.size);
        String _key761;
        int _val762;
        for (int _i763 = 0; _i763 < _map760.size; ++_i763)
        {
          _key761 = iprot.readString();
          _val762 = iprot.readI32();
          struct.approved_workers.put(_key761, _val762);
        }
      }
      struct.set_approved_workers_isSet(true);
    }
  }

}

