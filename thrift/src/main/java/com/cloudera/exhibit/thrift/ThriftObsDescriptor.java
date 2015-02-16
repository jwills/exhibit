package com.cloudera.exhibit.thrift;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ThriftObsDescriptor implements ObsDescriptor {
  private final List<FieldData> fields;

  public ThriftObsDescriptor(Class<? extends TBase> thriftClass) {
    Map<? extends TFieldIdEnum, FieldMetaData> mdm = FieldMetaData.getStructMetaDataMap(thriftClass);
    this.fields = Lists.newArrayListWithExpectedSize(mdm.size());
    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> e : mdm.entrySet()) {
      fields.add(new FieldData(e.getKey(), e.getValue()));
    }
  }

  @Override
  public int size() {
    return fields.size();
  }

  Object getFieldValue(int i, TBase tBase) {
    return tBase.getFieldValue(fields.get(i).id);
  }

  private static final Map<Byte, FieldType> TYPE_CLASSES = ImmutableMap.<Byte, FieldType>builder()
      .put(TType.BOOL, FieldType.BOOLEAN)
      .put(TType.DOUBLE, FieldType.DOUBLE)
      .put(TType.I16, FieldType.INTEGER)
      .put(TType.I32, FieldType.INTEGER)
      .put(TType.I64, FieldType.LONG)
      .put(TType.STRING, FieldType.STRING)
      .build();

  private static FieldType getFieldType(FieldMetaData metadata) {
    byte type = metadata.valueMetaData.type;
    if (TYPE_CLASSES.containsKey(type)) {
      return TYPE_CLASSES.get(type);
    } else {
      throw new UnsupportedOperationException("Unsupported Thrift type: " + type);
    }
  }

  @Override
  public Field get(int i) {
    FieldData fd = fields.get(i);
    return new Field(fd.metadata.fieldName, getFieldType(fd.metadata));
  }

  @Override
  public int indexOf(String name) {
    return 0;
  }

  @Override
  public Iterator<Field> iterator() {
    return Iterators.transform(fields.iterator(), new Function<FieldData, Field>() {
      @Override
      public Field apply(FieldData fieldData) {
        return new Field(fieldData.metadata.fieldName, getFieldType(fieldData.metadata));
      }
    });
  }

  private static class FieldData {
    TFieldIdEnum id;
    FieldMetaData metadata;

    public FieldData(TFieldIdEnum id, FieldMetaData metadata) {
      this.id = id;
      this.metadata = metadata;
    }
  }
}
