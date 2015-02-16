package com.cloudera.exhibit.avro;

import com.cloudera.exhibit.core.ObsDescriptor;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.avro.Schema;

import java.util.Iterator;
import java.util.List;

public class AvroObsDescriptor implements ObsDescriptor {

  private static Schema NULL = Schema.create(Schema.Type.NULL);

  private final Schema schema;

  public AvroObsDescriptor(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Field get(int index) {
    Schema.Field f = schema.getFields().get(index);
    return new Field(f.name(), getFieldType(f.schema()));
  }

  @Override
  public int indexOf(String name) {
    Schema.Field f = schema.getField(name);
    return f == null ? -1 : f.pos();
  }

  private static FieldType getFieldType(Schema s) {
    switch (s.getType()) {
      case BOOLEAN:
        return FieldType.BOOLEAN;
      case INT:
        return FieldType.INTEGER;
      case LONG:
        return FieldType.LONG;
      case FLOAT:
        return FieldType.FLOAT;
      case DOUBLE:
        return FieldType.DOUBLE;
      case STRING:
        return FieldType.STRING;
      case UNION:
        List<Schema> ut = s.getTypes();
        if (NULL.equals(ut.get(0))) {
          return getFieldType(ut.get(1));
        } else if (NULL.equals(ut.get(1))) {
          return getFieldType(ut.get(0));
        }
      default:
        throw new UnsupportedOperationException("Cannot convert Avro schema = " + s);
    }
  }

  @Override
  public int size() {
    return schema.getFields().size();
  }

  @Override
  public Iterator<Field> iterator() {
    return Iterators.transform(schema.getFields().iterator(), new Function<Schema.Field, Field>() {
      @Override
      public Field apply(Schema.Field f) {
        return new Field(f.name(), getFieldType(f.schema()));
      }
    });
  }
}
