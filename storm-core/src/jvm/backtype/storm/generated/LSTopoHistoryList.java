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
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-5-1")
public class LSTopoHistoryList implements org.apache.thrift.TBase<LSTopoHistoryList, LSTopoHistoryList._Fields>, java.io.Serializable, Cloneable, Comparable<LSTopoHistoryList> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LSTopoHistoryList");

  private static final org.apache.thrift.protocol.TField TOPO_HISTORY_FIELD_DESC = new org.apache.thrift.protocol.TField("topo_history", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LSTopoHistoryListStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LSTopoHistoryListTupleSchemeFactory());
  }

  private List<LSTopoHistory> topo_history; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TOPO_HISTORY((short)1, "topo_history");

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
        case 1: // TOPO_HISTORY
          return TOPO_HISTORY;
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
    tmpMap.put(_Fields.TOPO_HISTORY, new org.apache.thrift.meta_data.FieldMetaData("topo_history", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, LSTopoHistory.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LSTopoHistoryList.class, metaDataMap);
  }

  public LSTopoHistoryList() {
  }

  public LSTopoHistoryList(
    List<LSTopoHistory> topo_history)
  {
    this();
    this.topo_history = topo_history;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public LSTopoHistoryList(LSTopoHistoryList other) {
    if (other.is_set_topo_history()) {
      List<LSTopoHistory> __this__topo_history = new ArrayList<LSTopoHistory>(other.topo_history.size());
      for (LSTopoHistory other_element : other.topo_history) {
        __this__topo_history.add(new LSTopoHistory(other_element));
      }
      this.topo_history = __this__topo_history;
    }
  }

  public LSTopoHistoryList deepCopy() {
    return new LSTopoHistoryList(this);
  }

  @Override
  public void clear() {
    this.topo_history = null;
  }

  public int get_topo_history_size() {
    return (this.topo_history == null) ? 0 : this.topo_history.size();
  }

  public java.util.Iterator<LSTopoHistory> get_topo_history_iterator() {
    return (this.topo_history == null) ? null : this.topo_history.iterator();
  }

  public void add_to_topo_history(LSTopoHistory elem) {
    if (this.topo_history == null) {
      this.topo_history = new ArrayList<LSTopoHistory>();
    }
    this.topo_history.add(elem);
  }

  public List<LSTopoHistory> get_topo_history() {
    return this.topo_history;
  }

  public void set_topo_history(List<LSTopoHistory> topo_history) {
    this.topo_history = topo_history;
  }

  public void unset_topo_history() {
    this.topo_history = null;
  }

  /** Returns true if field topo_history is set (has been assigned a value) and false otherwise */
  public boolean is_set_topo_history() {
    return this.topo_history != null;
  }

  public void set_topo_history_isSet(boolean value) {
    if (!value) {
      this.topo_history = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TOPO_HISTORY:
      if (value == null) {
        unset_topo_history();
      } else {
        set_topo_history((List<LSTopoHistory>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TOPO_HISTORY:
      return get_topo_history();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TOPO_HISTORY:
      return is_set_topo_history();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof LSTopoHistoryList)
      return this.equals((LSTopoHistoryList)that);
    return false;
  }

  public boolean equals(LSTopoHistoryList that) {
    if (that == null)
      return false;

    boolean this_present_topo_history = true && this.is_set_topo_history();
    boolean that_present_topo_history = true && that.is_set_topo_history();
    if (this_present_topo_history || that_present_topo_history) {
      if (!(this_present_topo_history && that_present_topo_history))
        return false;
      if (!this.topo_history.equals(that.topo_history))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_topo_history = true && (is_set_topo_history());
    list.add(present_topo_history);
    if (present_topo_history)
      list.add(topo_history);

    return list.hashCode();
  }

  @Override
  public int compareTo(LSTopoHistoryList other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(is_set_topo_history()).compareTo(other.is_set_topo_history());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_topo_history()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.topo_history, other.topo_history);
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
    StringBuilder sb = new StringBuilder("LSTopoHistoryList(");
    boolean first = true;

    sb.append("topo_history:");
    if (this.topo_history == null) {
      sb.append("null");
    } else {
      sb.append(this.topo_history);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!is_set_topo_history()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'topo_history' is unset! Struct:" + toString());
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

  private static class LSTopoHistoryListStandardSchemeFactory implements SchemeFactory {
    public LSTopoHistoryListStandardScheme getScheme() {
      return new LSTopoHistoryListStandardScheme();
    }
  }

  private static class LSTopoHistoryListStandardScheme extends StandardScheme<LSTopoHistoryList> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, LSTopoHistoryList struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TOPO_HISTORY
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list598 = iprot.readListBegin();
                struct.topo_history = new ArrayList<LSTopoHistory>(_list598.size);
                LSTopoHistory _elem599;
                for (int _i600 = 0; _i600 < _list598.size; ++_i600)
                {
                  _elem599 = new LSTopoHistory();
                  _elem599.read(iprot);
                  struct.topo_history.add(_elem599);
                }
                iprot.readListEnd();
              }
              struct.set_topo_history_isSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, LSTopoHistoryList struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.topo_history != null) {
        oprot.writeFieldBegin(TOPO_HISTORY_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.topo_history.size()));
          for (LSTopoHistory _iter601 : struct.topo_history)
          {
            _iter601.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LSTopoHistoryListTupleSchemeFactory implements SchemeFactory {
    public LSTopoHistoryListTupleScheme getScheme() {
      return new LSTopoHistoryListTupleScheme();
    }
  }

  private static class LSTopoHistoryListTupleScheme extends TupleScheme<LSTopoHistoryList> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, LSTopoHistoryList struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.topo_history.size());
        for (LSTopoHistory _iter602 : struct.topo_history)
        {
          _iter602.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, LSTopoHistoryList struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list603 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.topo_history = new ArrayList<LSTopoHistory>(_list603.size);
        LSTopoHistory _elem604;
        for (int _i605 = 0; _i605 < _list603.size; ++_i605)
        {
          _elem604 = new LSTopoHistory();
          _elem604.read(iprot);
          struct.topo_history.add(_elem604);
        }
      }
      struct.set_topo_history_isSet(true);
    }
  }

}

