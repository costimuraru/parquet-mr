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

import com.google.common.collect.Maps;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.parquet.proto.test.MapProtobuf;

import java.io.IOException;
import java.util.Map;

public class ProtoToAthena {

    public static void main(String[] args) throws IOException {
        Message message = MapProtobuf.Log.getDefaultInstance();
        ProtoToAthena formatter = new ProtoToAthena();

        System.out.println(formatter.toAthenaSql(message, "cmuraru20", "s3://cmuraru-analytics/athena/test20/"));
    }

    public String toAthenaSql(Message message, String tableName, String s3Location) throws IOException {
        StringBuilder query = new StringBuilder()
                .append("CREATE EXTERNAL TABLE IF NOT EXISTS ")
                .append(tableName).append(" (\n");

        printMessage(message, query, false);

        query.append(")\n")
                .append("PARTITIONED BY (pdate string)\n")
                .append("STORED AS PARQUET\n")
                .append("LOCATION '").append(s3Location).append("'\n")
                .append("tblproperties (\"parquet.compress\"=\"GZIP\")").append(";\n\n")
                .append("MSCK REPAIR TABLE ").append(tableName).append(";\n\n")
                .append("SELECT * FROM ").append(tableName).append(" LIMIT 10;");
        return query.toString();
    }

    private void printMessage(Message message, StringBuilder stringBuilder, boolean inStruct) throws IOException {

        Map<FieldDescriptor, Object> fieldsToPrint = Maps.newLinkedHashMap();
        for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
            fieldsToPrint.put(field, message.getField(field));
        }

        boolean addSeparator = false;
        for (Map.Entry<FieldDescriptor, Object> field : fieldsToPrint.entrySet()) {
            if (addSeparator) {
                stringBuilder.append(",");
                if (!inStruct) {
                    stringBuilder.append("\n");
                }
            }
            printField(message, field.getKey(), field.getValue(), stringBuilder, inStruct);
            addSeparator = true;
        }
    }

    private void printField(Message message, FieldDescriptor field, Object value, StringBuilder stringBuilder, boolean inStruct) throws IOException {
        if (field.isMapField()) {
            printMapFieldValue(field, value, stringBuilder, inStruct, message);
        } else {
            printSingleFieldValue(field, value, stringBuilder, inStruct, message);
        }
    }

    private void printMapFieldValue(final FieldDescriptor field, final Object value, StringBuilder stringBuilder, boolean inStruct, Message message) throws IOException {
        stringBuilder
                .append(getFieldName(field, inStruct))
                .append("map<string,string>"); //TODO: add support for other maps
    }

    private void printSingleFieldValue(final FieldDescriptor field, final Object value, StringBuilder stringBuilder, boolean inStruct, Message message) throws IOException {
        stringBuilder.append(getFieldName(field, inStruct));

        if (field.isRepeated()) {
            stringBuilder.append("array<");
        }

        switch (field.getType()) {
            case INT32:
            case SINT32:
            case SFIXED32:
                stringBuilder.append("int");
                break;

            case INT64:
            case SINT64:
            case SFIXED64:
                stringBuilder.append("bigint");
                break;

            case FLOAT:
                stringBuilder.append("float");
                break;

            case DOUBLE:
                stringBuilder.append("double");
                break;

            case BOOL:
                stringBuilder.append("boolean");
                break;

            case UINT32:
            case FIXED32:
                stringBuilder.append("int");
                break;

            case UINT64:
            case FIXED64:
                stringBuilder.append("bigint");
                break;

            case STRING:
                stringBuilder.append("string");
                break;

            case BYTES:
                stringBuilder.append("binary");
                break;

            case ENUM:
                stringBuilder.append("string");
                break;

            case MESSAGE:
            case GROUP:
                stringBuilder.append("struct<");
                Message innerMessage = message.toBuilder().newBuilderForField(field).build();
                printMessage(innerMessage, stringBuilder, true);
                stringBuilder.append(">");
                break;
        }

        if (field.isRepeated()) {
            stringBuilder.append(">");
        }
    }

    private String getFieldName(FieldDescriptor field, boolean inStruct) {
        if (inStruct) {
            return "`" + field.getName() + "`" + ":";
        }

        return "`" + field.getName()+ "` ";
    }
}
