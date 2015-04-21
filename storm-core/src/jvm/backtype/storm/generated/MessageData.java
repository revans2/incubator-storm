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
public class MessageData extends org.apache.thrift.TUnion<MessageData, MessageData._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("MessageData");
  private static final org.apache.thrift.protocol.TField PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("path", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField PULSE_FIELD_DESC = new org.apache.thrift.protocol.TField("pulse", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField BOOLVAL_FIELD_DESC = new org.apache.thrift.protocol.TField("boolval", org.apache.thrift.protocol.TType.BOOL, (short)3);
  private static final org.apache.thrift.protocol.TField RECORDS_FIELD_DESC = new org.apache.thrift.protocol.TField("records", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField NODES_FIELD_DESC = new org.apache.thrift.protocol.TField("nodes", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField CONTROL_MESSAGE_FIELD_DESC = new org.apache.thrift.protocol.TField("control_message", org.apache.thrift.protocol.TType.STRING, (short)6);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PATH((short)1, "path"),
    PULSE((short)2, "pulse"),
    BOOLVAL((short)3, "boolval"),
    RECORDS((short)4, "records"),
    NODES((short)5, "nodes"),
    CONTROL_MESSAGE((short)6, "control_message");

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
        case 1: // PATH
          return PATH;
        case 2: // PULSE
          return PULSE;
        case 3: // BOOLVAL
          return BOOLVAL;
        case 4: // RECORDS
          return RECORDS;
        case 5: // NODES
          return NODES;
        case 6: // CONTROL_MESSAGE
          return CONTROL_MESSAGE;
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PATH, new org.apache.thrift.meta_data.FieldMetaData("path", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PULSE, new org.apache.thrift.meta_data.FieldMetaData("pulse", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "Pulse")));
    tmpMap.put(_Fields.BOOLVAL, new org.apache.thrift.meta_data.FieldMetaData("boolval", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.RECORDS, new org.apache.thrift.meta_data.FieldMetaData("records", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "HBRecords")));
    tmpMap.put(_Fields.NODES, new org.apache.thrift.meta_data.FieldMetaData("nodes", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRUCT        , "HBNodes")));
    tmpMap.put(_Fields.CONTROL_MESSAGE, new org.apache.thrift.meta_data.FieldMetaData("control_message", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(MessageData.class, metaDataMap);
  }

  public MessageData() {
    super();
  }

  public MessageData(_Fields setField, Object value) {
    super(setField, value);
  }

  public MessageData(MessageData other) {
    super(other);
  }
  public MessageData deepCopy() {
    return new MessageData(this);
  }

  public static MessageData path(String value) {
    MessageData x = new MessageData();
    x.set_path(value);
    return x;
  }

  public static MessageData pulse(Pulse value) {
    MessageData x = new MessageData();
    x.set_pulse(value);
    return x;
  }

  public static MessageData boolval(boolean value) {
    MessageData x = new MessageData();
    x.set_boolval(value);
    return x;
  }

  public static MessageData records(HBRecords value) {
    MessageData x = new MessageData();
    x.set_records(value);
    return x;
  }

  public static MessageData nodes(HBNodes value) {
    MessageData x = new MessageData();
    x.set_nodes(value);
    return x;
  }

  public static MessageData control_message(ByteBuffer value) {
    MessageData x = new MessageData();
    x.set_control_message(value);
    return x;
  }

  public static MessageData control_message(byte[] value) {
    MessageData x = new MessageData();
    x.set_control_message(ByteBuffer.wrap(Arrays.copyOf(value, value.length)));
    return x;
  }


  @Override
  protected void checkType(_Fields setField, Object value) throws ClassCastException {
    switch (setField) {
      case PATH:
        if (value instanceof String) {
          break;
        }
        throw new ClassCastException("Was expecting value of type String for field 'path', but got " + value.getClass().getSimpleName());
      case PULSE:
        if (value instanceof Pulse) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Pulse for field 'pulse', but got " + value.getClass().getSimpleName());
      case BOOLVAL:
        if (value instanceof Boolean) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Boolean for field 'boolval', but got " + value.getClass().getSimpleName());
      case RECORDS:
        if (value instanceof HBRecords) {
          break;
        }
        throw new ClassCastException("Was expecting value of type HBRecords for field 'records', but got " + value.getClass().getSimpleName());
      case NODES:
        if (value instanceof HBNodes) {
          break;
        }
        throw new ClassCastException("Was expecting value of type HBNodes for field 'nodes', but got " + value.getClass().getSimpleName());
      case CONTROL_MESSAGE:
        if (value instanceof ByteBuffer) {
          break;
        }
        throw new ClassCastException("Was expecting value of type ByteBuffer for field 'control_message', but got " + value.getClass().getSimpleName());
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case PATH:
          if (field.type == PATH_FIELD_DESC.type) {
            String path;
            path = iprot.readString();
            return path;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case PULSE:
          if (field.type == PULSE_FIELD_DESC.type) {
            Pulse pulse;
            pulse = new Pulse();
            pulse.read(iprot);
            return pulse;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case BOOLVAL:
          if (field.type == BOOLVAL_FIELD_DESC.type) {
            Boolean boolval;
            boolval = iprot.readBool();
            return boolval;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case RECORDS:
          if (field.type == RECORDS_FIELD_DESC.type) {
            HBRecords records;
            records = new HBRecords();
            records.read(iprot);
            return records;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case NODES:
          if (field.type == NODES_FIELD_DESC.type) {
            HBNodes nodes;
            nodes = new HBNodes();
            nodes.read(iprot);
            return nodes;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case CONTROL_MESSAGE:
          if (field.type == CONTROL_MESSAGE_FIELD_DESC.type) {
            ByteBuffer control_message;
            control_message = iprot.readBinary();
            return control_message;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case PATH:
        String path = (String)value_;
        oprot.writeString(path);
        return;
      case PULSE:
        Pulse pulse = (Pulse)value_;
        pulse.write(oprot);
        return;
      case BOOLVAL:
        Boolean boolval = (Boolean)value_;
        oprot.writeBool(boolval);
        return;
      case RECORDS:
        HBRecords records = (HBRecords)value_;
        records.write(oprot);
        return;
      case NODES:
        HBNodes nodes = (HBNodes)value_;
        nodes.write(oprot);
        return;
      case CONTROL_MESSAGE:
        ByteBuffer control_message = (ByteBuffer)value_;
        oprot.writeBinary(control_message);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case PATH:
          String path;
          path = iprot.readString();
          return path;
        case PULSE:
          Pulse pulse;
          pulse = new Pulse();
          pulse.read(iprot);
          return pulse;
        case BOOLVAL:
          Boolean boolval;
          boolval = iprot.readBool();
          return boolval;
        case RECORDS:
          HBRecords records;
          records = new HBRecords();
          records.read(iprot);
          return records;
        case NODES:
          HBNodes nodes;
          nodes = new HBNodes();
          nodes.read(iprot);
          return nodes;
        case CONTROL_MESSAGE:
          ByteBuffer control_message;
          control_message = iprot.readBinary();
          return control_message;
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case PATH:
        String path = (String)value_;
        oprot.writeString(path);
        return;
      case PULSE:
        Pulse pulse = (Pulse)value_;
        pulse.write(oprot);
        return;
      case BOOLVAL:
        Boolean boolval = (Boolean)value_;
        oprot.writeBool(boolval);
        return;
      case RECORDS:
        HBRecords records = (HBRecords)value_;
        records.write(oprot);
        return;
      case NODES:
        HBNodes nodes = (HBNodes)value_;
        nodes.write(oprot);
        return;
      case CONTROL_MESSAGE:
        ByteBuffer control_message = (ByteBuffer)value_;
        oprot.writeBinary(control_message);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case PATH:
        return PATH_FIELD_DESC;
      case PULSE:
        return PULSE_FIELD_DESC;
      case BOOLVAL:
        return BOOLVAL_FIELD_DESC;
      case RECORDS:
        return RECORDS_FIELD_DESC;
      case NODES:
        return NODES_FIELD_DESC;
      case CONTROL_MESSAGE:
        return CONTROL_MESSAGE_FIELD_DESC;
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public String get_path() {
    if (getSetField() == _Fields.PATH) {
      return (String)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'path' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_path(String value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.PATH;
    value_ = value;
  }

  public Pulse get_pulse() {
    if (getSetField() == _Fields.PULSE) {
      return (Pulse)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'pulse' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_pulse(Pulse value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.PULSE;
    value_ = value;
  }

  public boolean get_boolval() {
    if (getSetField() == _Fields.BOOLVAL) {
      return (Boolean)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'boolval' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_boolval(boolean value) {
    setField_ = _Fields.BOOLVAL;
    value_ = value;
  }

  public HBRecords get_records() {
    if (getSetField() == _Fields.RECORDS) {
      return (HBRecords)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'records' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_records(HBRecords value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.RECORDS;
    value_ = value;
  }

  public HBNodes get_nodes() {
    if (getSetField() == _Fields.NODES) {
      return (HBNodes)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'nodes' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_nodes(HBNodes value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.NODES;
    value_ = value;
  }

  public byte[] get_control_message() {
    set_control_message(org.apache.thrift.TBaseHelper.rightSize(buffer_for_control_message()));
    ByteBuffer b = buffer_for_control_message();
    return b == null ? null : b.array();
  }

  public ByteBuffer buffer_for_control_message() {
    if (getSetField() == _Fields.CONTROL_MESSAGE) {
      return org.apache.thrift.TBaseHelper.copyBinary((ByteBuffer)getFieldValue());
    } else {
      throw new RuntimeException("Cannot get field 'control_message' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void set_control_message(byte[] value) {
    set_control_message(ByteBuffer.wrap(Arrays.copyOf(value, value.length)));
  }

  public void set_control_message(ByteBuffer value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.CONTROL_MESSAGE;
    value_ = value;
  }

  public boolean is_set_path() {
    return setField_ == _Fields.PATH;
  }


  public boolean is_set_pulse() {
    return setField_ == _Fields.PULSE;
  }


  public boolean is_set_boolval() {
    return setField_ == _Fields.BOOLVAL;
  }


  public boolean is_set_records() {
    return setField_ == _Fields.RECORDS;
  }


  public boolean is_set_nodes() {
    return setField_ == _Fields.NODES;
  }


  public boolean is_set_control_message() {
    return setField_ == _Fields.CONTROL_MESSAGE;
  }


  public boolean equals(Object other) {
    if (other instanceof MessageData) {
      return equals((MessageData)other);
    } else {
      return false;
    }
  }

  public boolean equals(MessageData other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(MessageData other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();
    list.add(this.getClass().getName());
    org.apache.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      list.add(setField.getThriftFieldId());
      Object value = getFieldValue();
      if (value instanceof org.apache.thrift.TEnum) {
        list.add(((org.apache.thrift.TEnum)getFieldValue()).getValue());
      } else {
        list.add(value);
      }
    }
    return list.hashCode();
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


}
