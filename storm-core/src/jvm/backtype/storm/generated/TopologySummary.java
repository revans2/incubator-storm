/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package backtype.storm.generated;

import org.apache.commons.lang.builder.HashCodeBuilder;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologySummary implements org.apache.thrift7.TBase<TopologySummary, TopologySummary._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift7.protocol.TStruct STRUCT_DESC = new org.apache.thrift7.protocol.TStruct("TopologySummary");

  private static final org.apache.thrift7.protocol.TField ID_FIELD_DESC = new org.apache.thrift7.protocol.TField("id", org.apache.thrift7.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift7.protocol.TField NAME_FIELD_DESC = new org.apache.thrift7.protocol.TField("name", org.apache.thrift7.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift7.protocol.TField NUM_TASKS_FIELD_DESC = new org.apache.thrift7.protocol.TField("num_tasks", org.apache.thrift7.protocol.TType.I32, (short)3);
  private static final org.apache.thrift7.protocol.TField NUM_EXECUTORS_FIELD_DESC = new org.apache.thrift7.protocol.TField("num_executors", org.apache.thrift7.protocol.TType.I32, (short)4);
  private static final org.apache.thrift7.protocol.TField NUM_WORKERS_FIELD_DESC = new org.apache.thrift7.protocol.TField("num_workers", org.apache.thrift7.protocol.TType.I32, (short)5);
  private static final org.apache.thrift7.protocol.TField UPTIME_SECS_FIELD_DESC = new org.apache.thrift7.protocol.TField("uptime_secs", org.apache.thrift7.protocol.TType.I32, (short)6);
  private static final org.apache.thrift7.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift7.protocol.TField("status", org.apache.thrift7.protocol.TType.STRING, (short)7);
  private static final org.apache.thrift7.protocol.TField SCHED_STATUS_FIELD_DESC = new org.apache.thrift7.protocol.TField("sched_status", org.apache.thrift7.protocol.TType.STRING, (short)513);
  private static final org.apache.thrift7.protocol.TField OWNER_FIELD_DESC = new org.apache.thrift7.protocol.TField("owner", org.apache.thrift7.protocol.TType.STRING, (short)514);

  private String id; // required
  private String name; // required
  private int num_tasks; // required
  private int num_executors; // required
  private int num_workers; // required
  private int uptime_secs; // required
  private String status; // required
  private String sched_status; // required
  private String owner; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift7.TFieldIdEnum {
    ID((short)1, "id"),
    NAME((short)2, "name"),
    NUM_TASKS((short)3, "num_tasks"),
    NUM_EXECUTORS((short)4, "num_executors"),
    NUM_WORKERS((short)5, "num_workers"),
    UPTIME_SECS((short)6, "uptime_secs"),
    STATUS((short)7, "status"),
    SCHED_STATUS((short)513, "sched_status"),
    OWNER((short)514, "owner");

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
        case 1: // ID
          return ID;
        case 2: // NAME
          return NAME;
        case 3: // NUM_TASKS
          return NUM_TASKS;
        case 4: // NUM_EXECUTORS
          return NUM_EXECUTORS;
        case 5: // NUM_WORKERS
          return NUM_WORKERS;
        case 6: // UPTIME_SECS
          return UPTIME_SECS;
        case 7: // STATUS
          return STATUS;
        case 513: // SCHED_STATUS
          return SCHED_STATUS;
        case 514: // OWNER
          return OWNER;
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
  private static final int __NUM_TASKS_ISSET_ID = 0;
  private static final int __NUM_EXECUTORS_ISSET_ID = 1;
  private static final int __NUM_WORKERS_ISSET_ID = 2;
  private static final int __UPTIME_SECS_ISSET_ID = 3;
  private BitSet __isset_bit_vector = new BitSet(4);

  public static final Map<_Fields, org.apache.thrift7.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift7.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift7.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ID, new org.apache.thrift7.meta_data.FieldMetaData("id", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.STRING)));
    tmpMap.put(_Fields.NAME, new org.apache.thrift7.meta_data.FieldMetaData("name", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.STRING)));
    tmpMap.put(_Fields.NUM_TASKS, new org.apache.thrift7.meta_data.FieldMetaData("num_tasks", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_EXECUTORS, new org.apache.thrift7.meta_data.FieldMetaData("num_executors", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.I32)));
    tmpMap.put(_Fields.NUM_WORKERS, new org.apache.thrift7.meta_data.FieldMetaData("num_workers", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.I32)));
    tmpMap.put(_Fields.UPTIME_SECS, new org.apache.thrift7.meta_data.FieldMetaData("uptime_secs", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.I32)));
    tmpMap.put(_Fields.STATUS, new org.apache.thrift7.meta_data.FieldMetaData("status", org.apache.thrift7.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.STRING)));
    tmpMap.put(_Fields.SCHED_STATUS, new org.apache.thrift7.meta_data.FieldMetaData("sched_status", org.apache.thrift7.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.STRING)));
    tmpMap.put(_Fields.OWNER, new org.apache.thrift7.meta_data.FieldMetaData("owner", org.apache.thrift7.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift7.meta_data.FieldValueMetaData(org.apache.thrift7.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift7.meta_data.FieldMetaData.addStructMetaDataMap(TopologySummary.class, metaDataMap);
  }

  public TopologySummary() {
  }

  public TopologySummary(
    String id,
    String name,
    int num_tasks,
    int num_executors,
    int num_workers,
    int uptime_secs,
    String status)
  {
    this();
    this.id = id;
    this.name = name;
    this.num_tasks = num_tasks;
    set_num_tasks_isSet(true);
    this.num_executors = num_executors;
    set_num_executors_isSet(true);
    this.num_workers = num_workers;
    set_num_workers_isSet(true);
    this.uptime_secs = uptime_secs;
    set_uptime_secs_isSet(true);
    this.status = status;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TopologySummary(TopologySummary other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.is_set_id()) {
      this.id = other.id;
    }
    if (other.is_set_name()) {
      this.name = other.name;
    }
    this.num_tasks = other.num_tasks;
    this.num_executors = other.num_executors;
    this.num_workers = other.num_workers;
    this.uptime_secs = other.uptime_secs;
    if (other.is_set_status()) {
      this.status = other.status;
    }
    if (other.is_set_sched_status()) {
      this.sched_status = other.sched_status;
    }
    if (other.is_set_owner()) {
      this.owner = other.owner;
    }
  }

  public TopologySummary deepCopy() {
    return new TopologySummary(this);
  }

  @Override
  public void clear() {
    this.id = null;
    this.name = null;
    set_num_tasks_isSet(false);
    this.num_tasks = 0;
    set_num_executors_isSet(false);
    this.num_executors = 0;
    set_num_workers_isSet(false);
    this.num_workers = 0;
    set_uptime_secs_isSet(false);
    this.uptime_secs = 0;
    this.status = null;
    this.sched_status = null;
    this.owner = null;
  }

  public String get_id() {
    return this.id;
  }

  public void set_id(String id) {
    this.id = id;
  }

  public void unset_id() {
    this.id = null;
  }

  /** Returns true if field id is set (has been assigned a value) and false otherwise */
  public boolean is_set_id() {
    return this.id != null;
  }

  public void set_id_isSet(boolean value) {
    if (!value) {
      this.id = null;
    }
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

  public int get_num_tasks() {
    return this.num_tasks;
  }

  public void set_num_tasks(int num_tasks) {
    this.num_tasks = num_tasks;
    set_num_tasks_isSet(true);
  }

  public void unset_num_tasks() {
    __isset_bit_vector.clear(__NUM_TASKS_ISSET_ID);
  }

  /** Returns true if field num_tasks is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_tasks() {
    return __isset_bit_vector.get(__NUM_TASKS_ISSET_ID);
  }

  public void set_num_tasks_isSet(boolean value) {
    __isset_bit_vector.set(__NUM_TASKS_ISSET_ID, value);
  }

  public int get_num_executors() {
    return this.num_executors;
  }

  public void set_num_executors(int num_executors) {
    this.num_executors = num_executors;
    set_num_executors_isSet(true);
  }

  public void unset_num_executors() {
    __isset_bit_vector.clear(__NUM_EXECUTORS_ISSET_ID);
  }

  /** Returns true if field num_executors is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_executors() {
    return __isset_bit_vector.get(__NUM_EXECUTORS_ISSET_ID);
  }

  public void set_num_executors_isSet(boolean value) {
    __isset_bit_vector.set(__NUM_EXECUTORS_ISSET_ID, value);
  }

  public int get_num_workers() {
    return this.num_workers;
  }

  public void set_num_workers(int num_workers) {
    this.num_workers = num_workers;
    set_num_workers_isSet(true);
  }

  public void unset_num_workers() {
    __isset_bit_vector.clear(__NUM_WORKERS_ISSET_ID);
  }

  /** Returns true if field num_workers is set (has been assigned a value) and false otherwise */
  public boolean is_set_num_workers() {
    return __isset_bit_vector.get(__NUM_WORKERS_ISSET_ID);
  }

  public void set_num_workers_isSet(boolean value) {
    __isset_bit_vector.set(__NUM_WORKERS_ISSET_ID, value);
  }

  public int get_uptime_secs() {
    return this.uptime_secs;
  }

  public void set_uptime_secs(int uptime_secs) {
    this.uptime_secs = uptime_secs;
    set_uptime_secs_isSet(true);
  }

  public void unset_uptime_secs() {
    __isset_bit_vector.clear(__UPTIME_SECS_ISSET_ID);
  }

  /** Returns true if field uptime_secs is set (has been assigned a value) and false otherwise */
  public boolean is_set_uptime_secs() {
    return __isset_bit_vector.get(__UPTIME_SECS_ISSET_ID);
  }

  public void set_uptime_secs_isSet(boolean value) {
    __isset_bit_vector.set(__UPTIME_SECS_ISSET_ID, value);
  }

  public String get_status() {
    return this.status;
  }

  public void set_status(String status) {
    this.status = status;
  }

  public void unset_status() {
    this.status = null;
  }

  /** Returns true if field status is set (has been assigned a value) and false otherwise */
  public boolean is_set_status() {
    return this.status != null;
  }

  public void set_status_isSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public String get_sched_status() {
    return this.sched_status;
  }

  public void set_sched_status(String sched_status) {
    this.sched_status = sched_status;
  }

  public void unset_sched_status() {
    this.sched_status = null;
  }

  /** Returns true if field sched_status is set (has been assigned a value) and false otherwise */
  public boolean is_set_sched_status() {
    return this.sched_status != null;
  }

  public void set_sched_status_isSet(boolean value) {
    if (!value) {
      this.sched_status = null;
    }
  }

  public String get_owner() {
    return this.owner;
  }

  public void set_owner(String owner) {
    this.owner = owner;
  }

  public void unset_owner() {
    this.owner = null;
  }

  /** Returns true if field owner is set (has been assigned a value) and false otherwise */
  public boolean is_set_owner() {
    return this.owner != null;
  }

  public void set_owner_isSet(boolean value) {
    if (!value) {
      this.owner = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ID:
      if (value == null) {
        unset_id();
      } else {
        set_id((String)value);
      }
      break;

    case NAME:
      if (value == null) {
        unset_name();
      } else {
        set_name((String)value);
      }
      break;

    case NUM_TASKS:
      if (value == null) {
        unset_num_tasks();
      } else {
        set_num_tasks((Integer)value);
      }
      break;

    case NUM_EXECUTORS:
      if (value == null) {
        unset_num_executors();
      } else {
        set_num_executors((Integer)value);
      }
      break;

    case NUM_WORKERS:
      if (value == null) {
        unset_num_workers();
      } else {
        set_num_workers((Integer)value);
      }
      break;

    case UPTIME_SECS:
      if (value == null) {
        unset_uptime_secs();
      } else {
        set_uptime_secs((Integer)value);
      }
      break;

    case STATUS:
      if (value == null) {
        unset_status();
      } else {
        set_status((String)value);
      }
      break;

    case SCHED_STATUS:
      if (value == null) {
        unset_sched_status();
      } else {
        set_sched_status((String)value);
      }
      break;

    case OWNER:
      if (value == null) {
        unset_owner();
      } else {
        set_owner((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ID:
      return get_id();

    case NAME:
      return get_name();

    case NUM_TASKS:
      return Integer.valueOf(get_num_tasks());

    case NUM_EXECUTORS:
      return Integer.valueOf(get_num_executors());

    case NUM_WORKERS:
      return Integer.valueOf(get_num_workers());

    case UPTIME_SECS:
      return Integer.valueOf(get_uptime_secs());

    case STATUS:
      return get_status();

    case SCHED_STATUS:
      return get_sched_status();

    case OWNER:
      return get_owner();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ID:
      return is_set_id();
    case NAME:
      return is_set_name();
    case NUM_TASKS:
      return is_set_num_tasks();
    case NUM_EXECUTORS:
      return is_set_num_executors();
    case NUM_WORKERS:
      return is_set_num_workers();
    case UPTIME_SECS:
      return is_set_uptime_secs();
    case STATUS:
      return is_set_status();
    case SCHED_STATUS:
      return is_set_sched_status();
    case OWNER:
      return is_set_owner();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TopologySummary)
      return this.equals((TopologySummary)that);
    return false;
  }

  public boolean equals(TopologySummary that) {
    if (that == null)
      return false;

    boolean this_present_id = true && this.is_set_id();
    boolean that_present_id = true && that.is_set_id();
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (!this.id.equals(that.id))
        return false;
    }

    boolean this_present_name = true && this.is_set_name();
    boolean that_present_name = true && that.is_set_name();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_num_tasks = true;
    boolean that_present_num_tasks = true;
    if (this_present_num_tasks || that_present_num_tasks) {
      if (!(this_present_num_tasks && that_present_num_tasks))
        return false;
      if (this.num_tasks != that.num_tasks)
        return false;
    }

    boolean this_present_num_executors = true;
    boolean that_present_num_executors = true;
    if (this_present_num_executors || that_present_num_executors) {
      if (!(this_present_num_executors && that_present_num_executors))
        return false;
      if (this.num_executors != that.num_executors)
        return false;
    }

    boolean this_present_num_workers = true;
    boolean that_present_num_workers = true;
    if (this_present_num_workers || that_present_num_workers) {
      if (!(this_present_num_workers && that_present_num_workers))
        return false;
      if (this.num_workers != that.num_workers)
        return false;
    }

    boolean this_present_uptime_secs = true;
    boolean that_present_uptime_secs = true;
    if (this_present_uptime_secs || that_present_uptime_secs) {
      if (!(this_present_uptime_secs && that_present_uptime_secs))
        return false;
      if (this.uptime_secs != that.uptime_secs)
        return false;
    }

    boolean this_present_status = true && this.is_set_status();
    boolean that_present_status = true && that.is_set_status();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!this.status.equals(that.status))
        return false;
    }

    boolean this_present_sched_status = true && this.is_set_sched_status();
    boolean that_present_sched_status = true && that.is_set_sched_status();
    if (this_present_sched_status || that_present_sched_status) {
      if (!(this_present_sched_status && that_present_sched_status))
        return false;
      if (!this.sched_status.equals(that.sched_status))
        return false;
    }

    boolean this_present_owner = true && this.is_set_owner();
    boolean that_present_owner = true && that.is_set_owner();
    if (this_present_owner || that_present_owner) {
      if (!(this_present_owner && that_present_owner))
        return false;
      if (!this.owner.equals(that.owner))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_id = true && (is_set_id());
    builder.append(present_id);
    if (present_id)
      builder.append(id);

    boolean present_name = true && (is_set_name());
    builder.append(present_name);
    if (present_name)
      builder.append(name);

    boolean present_num_tasks = true;
    builder.append(present_num_tasks);
    if (present_num_tasks)
      builder.append(num_tasks);

    boolean present_num_executors = true;
    builder.append(present_num_executors);
    if (present_num_executors)
      builder.append(num_executors);

    boolean present_num_workers = true;
    builder.append(present_num_workers);
    if (present_num_workers)
      builder.append(num_workers);

    boolean present_uptime_secs = true;
    builder.append(present_uptime_secs);
    if (present_uptime_secs)
      builder.append(uptime_secs);

    boolean present_status = true && (is_set_status());
    builder.append(present_status);
    if (present_status)
      builder.append(status);

    boolean present_sched_status = true && (is_set_sched_status());
    builder.append(present_sched_status);
    if (present_sched_status)
      builder.append(sched_status);

    boolean present_owner = true && (is_set_owner());
    builder.append(present_owner);
    if (present_owner)
      builder.append(owner);

    return builder.toHashCode();
  }

  public int compareTo(TopologySummary other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TopologySummary typedOther = (TopologySummary)other;

    lastComparison = Boolean.valueOf(is_set_id()).compareTo(typedOther.is_set_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_id()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.id, typedOther.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_name()).compareTo(typedOther.is_set_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_name()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.name, typedOther.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_num_tasks()).compareTo(typedOther.is_set_num_tasks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_tasks()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.num_tasks, typedOther.num_tasks);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_num_executors()).compareTo(typedOther.is_set_num_executors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_executors()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.num_executors, typedOther.num_executors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_num_workers()).compareTo(typedOther.is_set_num_workers());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_num_workers()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.num_workers, typedOther.num_workers);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_uptime_secs()).compareTo(typedOther.is_set_uptime_secs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_uptime_secs()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.uptime_secs, typedOther.uptime_secs);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_status()).compareTo(typedOther.is_set_status());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_status()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.status, typedOther.status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_sched_status()).compareTo(typedOther.is_set_sched_status());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_sched_status()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.sched_status, typedOther.sched_status);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(is_set_owner()).compareTo(typedOther.is_set_owner());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (is_set_owner()) {
      lastComparison = org.apache.thrift7.TBaseHelper.compareTo(this.owner, typedOther.owner);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift7.protocol.TProtocol iprot) throws org.apache.thrift7.TException {
    org.apache.thrift7.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift7.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // ID
          if (field.type == org.apache.thrift7.protocol.TType.STRING) {
            this.id = iprot.readString();
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // NAME
          if (field.type == org.apache.thrift7.protocol.TType.STRING) {
            this.name = iprot.readString();
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // NUM_TASKS
          if (field.type == org.apache.thrift7.protocol.TType.I32) {
            this.num_tasks = iprot.readI32();
            set_num_tasks_isSet(true);
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 4: // NUM_EXECUTORS
          if (field.type == org.apache.thrift7.protocol.TType.I32) {
            this.num_executors = iprot.readI32();
            set_num_executors_isSet(true);
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 5: // NUM_WORKERS
          if (field.type == org.apache.thrift7.protocol.TType.I32) {
            this.num_workers = iprot.readI32();
            set_num_workers_isSet(true);
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 6: // UPTIME_SECS
          if (field.type == org.apache.thrift7.protocol.TType.I32) {
            this.uptime_secs = iprot.readI32();
            set_uptime_secs_isSet(true);
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 7: // STATUS
          if (field.type == org.apache.thrift7.protocol.TType.STRING) {
            this.status = iprot.readString();
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 513: // SCHED_STATUS
          if (field.type == org.apache.thrift7.protocol.TType.STRING) {
            this.sched_status = iprot.readString();
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 514: // OWNER
          if (field.type == org.apache.thrift7.protocol.TType.STRING) {
            this.owner = iprot.readString();
          } else { 
            org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift7.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
    validate();
  }

  public void write(org.apache.thrift7.protocol.TProtocol oprot) throws org.apache.thrift7.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.id != null) {
      oprot.writeFieldBegin(ID_FIELD_DESC);
      oprot.writeString(this.id);
      oprot.writeFieldEnd();
    }
    if (this.name != null) {
      oprot.writeFieldBegin(NAME_FIELD_DESC);
      oprot.writeString(this.name);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(NUM_TASKS_FIELD_DESC);
    oprot.writeI32(this.num_tasks);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(NUM_EXECUTORS_FIELD_DESC);
    oprot.writeI32(this.num_executors);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(NUM_WORKERS_FIELD_DESC);
    oprot.writeI32(this.num_workers);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(UPTIME_SECS_FIELD_DESC);
    oprot.writeI32(this.uptime_secs);
    oprot.writeFieldEnd();
    if (this.status != null) {
      oprot.writeFieldBegin(STATUS_FIELD_DESC);
      oprot.writeString(this.status);
      oprot.writeFieldEnd();
    }
    if (this.sched_status != null) {
      if (is_set_sched_status()) {
        oprot.writeFieldBegin(SCHED_STATUS_FIELD_DESC);
        oprot.writeString(this.sched_status);
        oprot.writeFieldEnd();
      }
    }
    if (this.owner != null) {
      if (is_set_owner()) {
        oprot.writeFieldBegin(OWNER_FIELD_DESC);
        oprot.writeString(this.owner);
        oprot.writeFieldEnd();
      }
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TopologySummary(");
    boolean first = true;

    sb.append("id:");
    if (this.id == null) {
      sb.append("null");
    } else {
      sb.append(this.id);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("num_tasks:");
    sb.append(this.num_tasks);
    first = false;
    if (!first) sb.append(", ");
    sb.append("num_executors:");
    sb.append(this.num_executors);
    first = false;
    if (!first) sb.append(", ");
    sb.append("num_workers:");
    sb.append(this.num_workers);
    first = false;
    if (!first) sb.append(", ");
    sb.append("uptime_secs:");
    sb.append(this.uptime_secs);
    first = false;
    if (!first) sb.append(", ");
    sb.append("status:");
    if (this.status == null) {
      sb.append("null");
    } else {
      sb.append(this.status);
    }
    first = false;
    if (is_set_sched_status()) {
      if (!first) sb.append(", ");
      sb.append("sched_status:");
      if (this.sched_status == null) {
        sb.append("null");
      } else {
        sb.append(this.sched_status);
      }
      first = false;
    }
    if (is_set_owner()) {
      if (!first) sb.append(", ");
      sb.append("owner:");
      if (this.owner == null) {
        sb.append("null");
      } else {
        sb.append(this.owner);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift7.TException {
    // check for required fields
    if (!is_set_id()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'id' is unset! Struct:" + toString());
    }

    if (!is_set_name()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'name' is unset! Struct:" + toString());
    }

    if (!is_set_num_tasks()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'num_tasks' is unset! Struct:" + toString());
    }

    if (!is_set_num_executors()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'num_executors' is unset! Struct:" + toString());
    }

    if (!is_set_num_workers()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'num_workers' is unset! Struct:" + toString());
    }

    if (!is_set_uptime_secs()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'uptime_secs' is unset! Struct:" + toString());
    }

    if (!is_set_status()) {
      throw new org.apache.thrift7.protocol.TProtocolException("Required field 'status' is unset! Struct:" + toString());
    }

  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift7.protocol.TCompactProtocol(new org.apache.thrift7.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift7.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift7.protocol.TCompactProtocol(new org.apache.thrift7.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift7.TException te) {
      throw new java.io.IOException(te);
    }
  }

}

