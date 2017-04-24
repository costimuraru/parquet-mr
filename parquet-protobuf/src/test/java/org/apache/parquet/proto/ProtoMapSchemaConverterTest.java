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

import com.google.protobuf.Message;
import org.apache.parquet.proto.test.MapProtobuf;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ProtoMapSchemaConverterTest {

  /**
   * Converts given pbClass to parquet schema and compares it with expected parquet schema.
   */
  private void testConversion(Class<? extends Message> pbClass, String parquetSchemaString) throws
          Exception {
    ProtoSchemaConverter protoSchemaConverter = new ProtoSchemaConverter();
    MessageType schema = protoSchemaConverter.convert(pbClass);
    MessageType expectedMT = MessageTypeParser.parseMessageType(parquetSchemaString);
    assertEquals(expectedMT.toString(), schema.toString());
  }


  /**
   * Tests that all protocol buffer datatypes are converted to correct parquet datatypes.
   */
  @Test
  public void testConvertMap() throws Exception {
    String expectedSchema =
      "message TestProtobuf.MyMessage {\n" +
        "  required int32 my_id = 1;\n" +
        "  required group my_map (MAP) = 2 {\n" +
        "    repeated group map (MAP_KEY_VALUE) {\n" +
        "      required binary key (UTF8);\n" +
        "      required double value;\n" +
        "    }\n" +
        "  }\n" +
        "}";

    testConversion(MapProtobuf.Log.class, expectedSchema);
  }

}
