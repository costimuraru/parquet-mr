/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.parquet.schema.*;
import org.apache.parquet.schema.Types.Builder;
import org.apache.parquet.schema.Types.GroupBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.parquet.schema.OriginalType.ENUM;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * <p/>
 * Converts a Protocol Buffer Descriptor into a Parquet schema.
 *
 * @author Lukas Nalezenec
 */
public class ProtoSchemaConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoSchemaConverter.class);

  public MessageType convert(Class<? extends Message> protobufClass) {
    LOG.debug("Converting protocol buffer class \"" + protobufClass + "\" to parquet schema.");
    Descriptors.Descriptor descriptor = Protobufs.getMessageDescriptor(protobufClass);
    MessageType messageType =
        convertFields(Types.buildMessage(), descriptor.getFields())
        .named(descriptor.getFullName());
    LOG.debug("Converter info:\n " + descriptor.toProto() + " was converted to \n" + messageType);
    return messageType;
  }

  /* Iterates over list of fields. **/
  private <T> GroupBuilder<T> convertFields(GroupBuilder<T> groupBuilder, List<Descriptors.FieldDescriptor> fieldDescriptors) {
    for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
      if (fieldDescriptor.isMapField() || fieldDescriptor.isRepeated()) {
        groupBuilder =
          addField(fieldDescriptor, groupBuilder).named("value");
      } else {
        groupBuilder =
          addField(fieldDescriptor, groupBuilder)
            .id(fieldDescriptor.getNumber())
            .named(fieldDescriptor.getName());
      }
    }
    return groupBuilder;
  }

  private Type.Repetition getRepetition(Descriptors.FieldDescriptor descriptor) {
    if (descriptor.isRequired()) {
      return Type.Repetition.REQUIRED;
    } else if (descriptor.isRepeated()) {
      return Type.Repetition.REPEATED;
    } else {
      return Type.Repetition.OPTIONAL;
    }
  }

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(Descriptors.FieldDescriptor descriptor, GroupBuilder<T> builder) {
    Type.Repetition repetition = getRepetition(descriptor);
    JavaType javaType = descriptor.getJavaType();
    switch (javaType) {
      case BOOLEAN: return builder.primitive(BOOLEAN, repetition);
      case INT: return builder.primitive(INT32, repetition);
      case LONG: return builder.primitive(INT64, repetition);
      case FLOAT: return builder.primitive(FLOAT, repetition);
      case DOUBLE: return builder.primitive(DOUBLE, repetition);
      case BYTE_STRING: return builder.primitive(BINARY, repetition);
      case STRING: return builder.primitive(BINARY, repetition).as(UTF8);
      case MESSAGE: {
        if (descriptor.isMapField()) {
          builder.addField(mapAsGroup(descriptor, repetition));
        } else if (descriptor.isRepeated()) {
          builder.addField(listAsGroup(descriptor, repetition));
        } else {
          GroupBuilder<GroupBuilder<T>> group = builder.group(repetition);
          convertFields(group, descriptor.getMessageType().getFields());
          return group;
        }
      }
      case ENUM: return builder.primitive(BINARY, repetition).as(ENUM);
      default:
        throw new UnsupportedOperationException("Cannot convert Protocol Buffer: unknown type " + javaType);
    }
  }

  private GroupType mapAsGroup(Descriptors.FieldDescriptor descriptor, Type.Repetition repetition) {
    List<Descriptors.FieldDescriptor> fields = descriptor.getMessageType().getFields();
    if (fields.size() != 2) {
      throw new RuntimeException("Expected two fields: key/value, but got: " + fields);
    }

    Types.PrimitiveBuilder<PrimitiveType> primitiveBuilder = getPrimitive(fields.get(1), Type.Repetition.REQUIRED);
    return ConversionPatterns.stringKeyMapType(repetition, descriptor.getName(), primitiveBuilder.named("value"));
  }

  private Types.PrimitiveBuilder<PrimitiveType> getPrimitive(Descriptors.FieldDescriptor fieldDescriptor, Type.Repetition repetition) {
    switch(fieldDescriptor.getType()) {
      case INT32:
        return Types.primitive(INT32, repetition);
      case INT64:
        return Types.primitive(INT64, repetition);
      case STRING:
        return Types.primitive(BINARY, repetition).as(UTF8);
      case DOUBLE:
        return Types.primitive(DOUBLE, repetition);
      default:
        throw new RuntimeException("Need to finish the implementation here.");
    }
  }

  private GroupType listAsGroup(Descriptors.FieldDescriptor descriptor, Type.Repetition repetition) {
    Descriptors.FieldDescriptor.Type mapValueType = descriptor.getMessageType().getFields().get(1).getType();

    Types.PrimitiveBuilder<PrimitiveType> primitiveBuilder;
    switch(mapValueType) {
      case INT32:
        primitiveBuilder = Types.primitive(INT32, repetition);
        break;
      case INT64:
        primitiveBuilder = Types.primitive(INT64, repetition);
        break;
      case STRING:
        primitiveBuilder = Types.primitive(BINARY, repetition).as(UTF8);
        break;
      case DOUBLE:
        primitiveBuilder = Types.primitive(DOUBLE, repetition);
        break;
      default:
        throw new RuntimeException("Need to finish the implementation here.");
    }

//    return ConversionPatterns.stringKeyMapType(repetition, descriptor.getName(), primitiveBuilder.named("value"));
    return ConversionPatterns.listOfElements(repetition, descriptor.getName(),
      primitiveBuilder.named("value"));
  }

}
