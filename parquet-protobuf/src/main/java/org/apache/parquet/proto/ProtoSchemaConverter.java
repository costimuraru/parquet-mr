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
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
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
      groupBuilder =
        addField(fieldDescriptor, groupBuilder)
          .id(fieldDescriptor.getNumber())
          .named(fieldDescriptor.getName());
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

  private <T> Builder<? extends Builder<?, GroupBuilder<T>>, GroupBuilder<T>> addField(Descriptors.FieldDescriptor descriptor, final GroupBuilder<T> builder) {
    Type.Repetition repetition = getRepetition(descriptor);
    JavaType javaType = descriptor.getJavaType();

    if (descriptor.isRepeated() && !descriptor.isMapField() && descriptor.getJavaType() != JavaType.MESSAGE) {
      GroupBuilder<GroupBuilder<T>> tier1 = builder.group(Type.Repetition.REQUIRED).as(OriginalType.LIST);
      PrimitiveTypeName primitiveType = getPrimitiveType(descriptor);
      if (primitiveType == BINARY) {
        OriginalType originalType = descriptor.getJavaType() == JavaType.ENUM ? ENUM : UTF8;
        return tier1
          .primitive(primitiveType, Type.Repetition.REPEATED).as(originalType).named("array");
      } else {
        return tier1
          .primitive(primitiveType, Type.Repetition.REPEATED).named("array");
      }
    }

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
          List<Descriptors.FieldDescriptor> fields = descriptor.getMessageType().getFields();
          if (fields.size() != 2) {
            throw new RuntimeException("Expected two fields: key/value, but got: " + fields);
          }

          Descriptors.FieldDescriptor mapEntryValueField = fields.get(1);
          GroupBuilder<GroupBuilder<T>> tier1 = builder.group(Type.Repetition.REQUIRED).as(OriginalType.MAP);
          GroupBuilder<GroupBuilder<GroupBuilder<T>>> tier2 = tier1.group(Type.Repetition.REPEATED).as(OriginalType.MAP_KEY_VALUE);
          PrimitiveTypeName primitiveType = getPrimitiveType(mapEntryValueField);
          GroupBuilder<GroupBuilder<GroupBuilder<T>>> tier3 = tier2
            .primitive(BINARY, Type.Repetition.REQUIRED).as(UTF8).named("key");

          if (primitiveType == BINARY) {
            OriginalType originalType = mapEntryValueField.getJavaType() == JavaType.ENUM ? ENUM : UTF8;
            tier3 = tier3
              .primitive(primitiveType, Type.Repetition.REQUIRED).as(originalType).named("value");
          } else {
            tier3 = tier3
              .primitive(primitiveType, Type.Repetition.REQUIRED).named("value");
          }

          return tier3.named("map");
        } else if (descriptor.isRepeated()) {

          GroupBuilder<GroupBuilder<T>> tier1 = builder.group(Type.Repetition.REQUIRED).as(OriginalType.LIST);
          GroupBuilder<GroupBuilder<GroupBuilder<T>>> tier2 = tier1.group(Type.Repetition.REPEATED);

          convertFields(tier2, descriptor.getMessageType().getFields());

          return tier2.named("array");
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

  private PrimitiveTypeName getPrimitiveType(Descriptors.FieldDescriptor fieldDescriptor) {
    JavaType javaType = fieldDescriptor.getJavaType();
    switch(javaType) {
      case INT:
        return PrimitiveTypeName.INT32;
      case LONG:
        return PrimitiveTypeName.INT64;
      case STRING:
        return PrimitiveTypeName.BINARY;
      case DOUBLE:
        return PrimitiveTypeName.DOUBLE;
      case BOOLEAN:
        return PrimitiveTypeName.BOOLEAN;
      case FLOAT:
        return PrimitiveTypeName.FLOAT;
      case ENUM:
        return PrimitiveTypeName.BINARY;
      default:
        throw new RuntimeException("Need to finish the implementation here. No mapping for: " + javaType);
    }
  }


}
