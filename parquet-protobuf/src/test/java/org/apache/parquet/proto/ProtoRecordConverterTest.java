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

import com.google.protobuf.ByteString;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.parquet.tools.read.SimpleRecord.NameValue;
import org.junit.Test;
import org.apache.parquet.proto.test.TestProto3;
import org.apache.parquet.proto.test.TestProto2;

import java.util.List;

import static org.apache.parquet.proto.TestUtils.writeAndReadParquet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.apache.parquet.proto.TestUtils.testData;

public class ProtoRecordConverterTest {

  @Test
  public void testProto2AllTypes() throws Exception {
    TestProto2.SchemaConverterAllDatatypes.Builder data;
    data = TestProto2.SchemaConverterAllDatatypes.newBuilder();

    data.setOptionalBool(true);
    data.setOptionalBytes(ByteString.copyFrom("someText", "UTF-8"));
    data.setOptionalDouble(0.577);
    data.setOptionalFloat(3.1415f);
    data.setOptionalEnum(TestProto2.SchemaConverterAllDatatypes.TestEnum.FIRST);
    data.setOptionalFixed32(1000 * 1000 * 1);
    data.setOptionalFixed64(1000 * 1000 * 1000 * 2);
    data.setOptionalInt32(1000 * 1000 * 3);
    data.setOptionalInt64(1000L * 1000 * 1000 * 4);
    data.setOptionalSFixed32(1000 * 1000 * 5);
    data.setOptionalSFixed64(1000L * 1000 * 1000 * 6);
    data.setOptionalSInt32(1000 * 1000 * 56);
    data.setOptionalSInt64(1000L * 1000 * 1000 * 7);
    data.setOptionalString("Good Will Hunting");
    data.setOptionalUInt32(1000 * 1000 * 8);
    data.setOptionalUInt64(1000L * 1000 * 1000 * 9);
    data.getOptionalMessageBuilder().setSomeId(1984);
    data.getPbGroupBuilder().setGroupInt(1492);

    TestProto2.SchemaConverterAllDatatypes dataBuilt = data.build();
    data.clear();

    List<TestProto2.SchemaConverterAllDatatypes> result;
    result = testData(false, dataBuilt);

    //data are fully checked in testData function. Lets do one more check.
    TestProto2.SchemaConverterAllDatatypes o = result.get(0);
    assertEquals("Good Will Hunting", o.getOptionalString());

    assertEquals(true, o.getOptionalBool());
    assertEquals(ByteString.copyFrom("someText", "UTF-8"), o.getOptionalBytes());
    assertEquals(0.577, o.getOptionalDouble(), 0.00001);
    assertEquals(3.1415f, o.getOptionalFloat(), 0.00001);
    assertEquals(TestProto2.SchemaConverterAllDatatypes.TestEnum.FIRST, o.getOptionalEnum());
    assertEquals(1000 * 1000 * 1, o.getOptionalFixed32());
    assertEquals(1000 * 1000 * 1000 * 2, o.getOptionalFixed64());
    assertEquals(1000 * 1000 * 3, o.getOptionalInt32());
    assertEquals(1000L * 1000 * 1000 * 4, o.getOptionalInt64());
    assertEquals(1000 * 1000 * 5, o.getOptionalSFixed32());
    assertEquals(1000L * 1000 * 1000 * 6, o.getOptionalSFixed64());
    assertEquals(1000 * 1000 * 56, o.getOptionalSInt32());
    assertEquals(1000L * 1000 * 1000 * 7, o.getOptionalSInt64());
    assertEquals(1000 * 1000 * 8, o.getOptionalUInt32());
    assertEquals(1000L * 1000 * 1000 * 9, o.getOptionalUInt64());
    assertEquals(1984, o.getOptionalMessage().getSomeId());
    assertEquals(1492, o.getPbGroup().getGroupInt());
  }

  @Test
  public void testProto3AllTypes() throws Exception {
    TestProto3.SchemaConverterAllDatatypes.Builder data;
    data = TestProto3.SchemaConverterAllDatatypes.newBuilder();

    data.setOptionalBool(true);
    data.setOptionalBytes(ByteString.copyFrom("someText", "UTF-8"));
    data.setOptionalDouble(0.577);
    data.setOptionalFloat(3.1415f);
    data.setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
    data.setOptionalFixed32(1000 * 1000 * 1);
    data.setOptionalFixed64(1000 * 1000 * 1000 * 2);
    data.setOptionalInt32(1000 * 1000 * 3);
    data.setOptionalInt64(1000L * 1000 * 1000 * 4);
    data.setOptionalSFixed32(1000 * 1000 * 5);
    data.setOptionalSFixed64(1000L * 1000 * 1000 * 6);
    data.setOptionalSInt32(1000 * 1000 * 56);
    data.setOptionalSInt64(1000L * 1000 * 1000 * 7);
    data.setOptionalString("Good Will Hunting");
    data.setOptionalUInt32(1000 * 1000 * 8);
    data.setOptionalUInt64(1000L * 1000 * 1000 * 9);
    data.getOptionalMessageBuilder().setSomeId(1984);
    data.setEmptyString("");

    TestProto3.SchemaConverterAllDatatypes dataBuilt = data.build();
    data.clear();

    List<TestProto3.SchemaConverterAllDatatypes> result;
    result = testData(true, dataBuilt);

    //data are fully checked in testData function. Lets do one more check.
    TestProto3.SchemaConverterAllDatatypes o = result.get(0);
    assertEquals("Good Will Hunting", o.getOptionalString());

    assertEquals(true, o.getOptionalBool());
    assertEquals(ByteString.copyFrom("someText", "UTF-8"), o.getOptionalBytes());
    assertEquals(0.577, o.getOptionalDouble(), 0.00001);
    assertEquals(3.1415f, o.getOptionalFloat(), 0.00001);
    assertEquals(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST, o.getOptionalEnum());
    assertEquals(1000 * 1000 * 1, o.getOptionalFixed32());
    assertEquals(1000 * 1000 * 1000 * 2, o.getOptionalFixed64());
    assertEquals(1000 * 1000 * 3, o.getOptionalInt32());
    assertEquals(1000L * 1000 * 1000 * 4, o.getOptionalInt64());
    assertEquals(1000 * 1000 * 5, o.getOptionalSFixed32());
    assertEquals(1000L * 1000 * 1000 * 6, o.getOptionalSFixed64());
    assertEquals(1000 * 1000 * 56, o.getOptionalSInt32());
    assertEquals(1000L * 1000 * 1000 * 7, o.getOptionalSInt64());
    assertEquals(1000 * 1000 * 8, o.getOptionalUInt32());
    assertEquals(1000L * 1000 * 1000 * 9, o.getOptionalUInt64());
    assertEquals(1984, o.getOptionalMessage().getSomeId());
  }

  @Test
  public void testProto2AllTypesMultiple() throws Exception {
    int count = 100;
    TestProto2.SchemaConverterAllDatatypes[] input = new TestProto2.SchemaConverterAllDatatypes[count];

    for (int i = 0; i < count; i++) {
      TestProto2.SchemaConverterAllDatatypes.Builder d = TestProto2.SchemaConverterAllDatatypes.newBuilder();

      if (i % 2 != 0) d.setOptionalBool(true);
      if (i % 3 != 0) d.setOptionalBytes(ByteString.copyFrom("someText " + i, "UTF-8"));
      if (i % 4 != 0) d.setOptionalDouble(0.577 * i);
      if (i % 5 != 0) d.setOptionalFloat(3.1415f * i);
      if (i % 6 != 0) d.setOptionalEnum(TestProto2.SchemaConverterAllDatatypes.TestEnum.FIRST);
      if (i % 7 != 0) d.setOptionalFixed32(1000 * i * 1);
      if (i % 8 != 0) d.setOptionalFixed64(1000 * i * 1000 * 2);
      if (i % 9 != 0) d.setOptionalInt32(1000 * i * 3);
      if (i % 2 != 1) d.setOptionalSFixed32(1000 * i * 5);
      if (i % 3 != 1) d.setOptionalSFixed64(1000 * i * 1000 * 6);
      if (i % 4 != 1) d.setOptionalSInt32(1000 * i * 56);
      if (i % 5 != 1) d.setOptionalSInt64(1000 * i * 1000 * 7);
      if (i % 6 != 1) d.setOptionalString("Good Will Hunting " + i);
      if (i % 7 != 1) d.setOptionalUInt32(1000 * i * 8);
      if (i % 8 != 1) d.setOptionalUInt64(1000 * i * 1000 * 9);
      if (i % 9 != 1) d.getOptionalMessageBuilder().setSomeId(1984 * i);
      if (i % 2 != 1) d.getPbGroupBuilder().setGroupInt(1492 * i);
      if (i % 3 != 1) d.setOptionalInt64(1000 * i * 1000 * 4);
      input[i] = d.build();
    }

    List<TestProto2.SchemaConverterAllDatatypes> result;
    result = testData(false, input);

    //data are fully checked in testData function. Lets do one more check.
    assertEquals("Good Will Hunting 0", result.get(0).getOptionalString());
    assertEquals("Good Will Hunting 90", result.get(90).getOptionalString());
  }

  @Test
  public void testProto3AllTypesMultiple() throws Exception {
    int count = 100;
    TestProto3.SchemaConverterAllDatatypes[] input = new TestProto3.SchemaConverterAllDatatypes[count];

    for (int i = 0; i < count; i++) {
      TestProto3.SchemaConverterAllDatatypes.Builder d = TestProto3.SchemaConverterAllDatatypes.newBuilder();

      if (i % 2 != 0) d.setOptionalBool(true);
      if (i % 3 != 0) d.setOptionalBytes(ByteString.copyFrom("someText " + i, "UTF-8"));
      if (i % 4 != 0) d.setOptionalDouble(0.577 * i);
      if (i % 5 != 0) d.setOptionalFloat(3.1415f * i);
      if (i % 6 != 0) d.setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST);
      if (i % 7 != 0) d.setOptionalFixed32(1000 * i * 1);
      if (i % 8 != 0) d.setOptionalFixed64(1000 * i * 1000 * 2);
      if (i % 9 != 0) d.setOptionalInt32(1000 * i * 3);
      if (i % 2 != 1) d.setOptionalSFixed32(1000 * i * 5);
      if (i % 3 != 1) d.setOptionalSFixed64(1000 * i * 1000 * 6);
      if (i % 4 != 1) d.setOptionalSInt32(1000 * i * 56);
      if (i % 5 != 1) d.setOptionalSInt64(1000 * i * 1000 * 7);
      if (i % 6 != 1) d.setOptionalString("Good Will Hunting " + i);
      if (i % 7 != 1) d.setOptionalUInt32(1000 * i * 8);
      if (i % 8 != 1) d.setOptionalUInt64(1000 * i * 1000 * 9);
      if (i % 9 != 1) d.getOptionalMessageBuilder().setSomeId(1984 * i);
      if (i % 3 != 1) d.setOptionalInt64(1000 * i * 1000 * 4);
      input[i] = d.build();
    }

    List<TestProto3.SchemaConverterAllDatatypes> result;
    result = testData(true, input);

    //data are fully checked in testData function. Lets do one more check.
    assertEquals("Good Will Hunting 0", result.get(0).getOptionalString());
    assertEquals("Good Will Hunting 90", result.get(90).getOptionalString());
  }

  @Test
  public void testProto2Defaults() throws Exception {
    TestProto2.SchemaConverterAllDatatypes.Builder data;
    data = TestProto2.SchemaConverterAllDatatypes.newBuilder();

    List<TestProto2.SchemaConverterAllDatatypes> result = testData(false, data.build());
    TestProto2.SchemaConverterAllDatatypes message = result.get(0);
    assertEquals("", message.getOptionalString());
    assertEquals(false, message.getOptionalBool());
    assertEquals(0, message.getOptionalFixed32());
  }

  @Test
  public void testProto3Defaults() throws Exception {
    TestProto3.SchemaConverterAllDatatypes data = TestProto3.SchemaConverterAllDatatypes.newBuilder()
      .setOptionalEnum(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST)
      .setOptionalFixed32(12).build();

    List<SimpleRecord> res = writeAndReadParquet(true, TestProto3.SchemaConverterAllDatatypes.class, data);
    SimpleRecord msg = res.get(0);
    final byte[] optionalEnumBinary2 = (byte[]) getValue(msg, "optionalEnum");
    final String optionalEnumValue2 = Binary.fromConstantByteArray(optionalEnumBinary2).toStringUsingUTF8();
    assertEquals(12, getValue(msg, "optionalFixed32"));
    assertEquals(TestProto3.SchemaConverterAllDatatypes.TestEnum.FIRST.toString(), optionalEnumValue2);
    assertEquals("", getValue(msg, "optionalString"));
    assertEquals(false, getValue(msg, "optionalBool"));
  }

  private static Object getValue(final SimpleRecord msg, final String name) {
    for (NameValue value : msg.getValues()) {
      if (value.getName().equals(name)) {
        return value.getValue();
      }
    }
    return null;
  }

  @Test
  public void testProto2RepeatedMessages() throws Exception {
    TestProto2.TopMessage.Builder top = TestProto2.TopMessage.newBuilder();
    top.addInnerBuilder().setOne("First inner");
    top.addInnerBuilder().setTwo("Second inner");
    top.addInnerBuilder().setThree("Third inner");

    TestProto2.TopMessage result = testData(false, top.build()).get(0);

    assertEquals(3, result.getInnerCount());

    TestProto2.InnerMessage first = result.getInner(0);
    TestProto2.InnerMessage second = result.getInner(1);
    TestProto2.InnerMessage third = result.getInner(2);

    assertEquals("First inner", first.getOne());
    assertFalse(first.hasTwo());
    assertFalse(first.hasThree());

    assertEquals("Second inner", second.getTwo());
    assertFalse(second.hasOne());
    assertFalse(second.hasThree());

    assertEquals("Third inner", third.getThree());
    assertFalse(third.hasOne());
    assertFalse(third.hasTwo());
  }

  @Test
  public void testProto3RepeatedMessages() throws Exception {
    TestProto3.TopMessage.Builder top = TestProto3.TopMessage.newBuilder();
    top.addInnerBuilder().setOne("First inner");
    top.addInnerBuilder().setTwo("Second inner");
    top.addInnerBuilder().setThree("Third inner");

    TestProto3.TopMessage result = testData(true, top.build()).get(0);

    assertEquals(3, result.getInnerCount());

    TestProto3.InnerMessage first = result.getInner(0);
    TestProto3.InnerMessage second = result.getInner(1);
    TestProto3.InnerMessage third = result.getInner(2);

    assertEquals("First inner", first.getOne());
    assertTrue(first.getTwo().isEmpty());
    assertTrue(first.getThree().isEmpty());

    assertEquals("Second inner", second.getTwo());
    assertTrue(second.getOne().isEmpty());
    assertTrue(second.getThree().isEmpty());

    assertEquals("Third inner", third.getThree());
    assertTrue(third.getOne().isEmpty());
    assertTrue(third.getTwo().isEmpty());
  }

  @Test
  public void testProto2RepeatedInt() throws Exception {
    TestProto2.RepeatedIntMessage.Builder top = TestProto2.RepeatedIntMessage.newBuilder();

    top.addRepeatedInt(1);
    top.addRepeatedInt(2);
    top.addRepeatedInt(3);

    TestProto2.RepeatedIntMessage result = testData(false, top.build()).get(0);

    assertEquals(3, result.getRepeatedIntCount());

    assertEquals(1, result.getRepeatedInt(0));
    assertEquals(2, result.getRepeatedInt(1));
    assertEquals(3, result.getRepeatedInt(2));
  }

  @Test
  public void testProto3RepeatedInt() throws Exception {
    TestProto3.RepeatedIntMessage.Builder top = TestProto3.RepeatedIntMessage.newBuilder();

    top.addRepeatedInt(1);
    top.addRepeatedInt(2);
    top.addRepeatedInt(3);

    TestProto3.RepeatedIntMessage result = testData(true, top.build()).get(0);

    assertEquals(3, result.getRepeatedIntCount());

    assertEquals(1, result.getRepeatedInt(0));
    assertEquals(2, result.getRepeatedInt(1));
    assertEquals(3, result.getRepeatedInt(2));
  }

  @Test
  public void testProto2LargeProtobufferFieldId() throws Exception {
    TestProto2.HighIndexMessage.Builder builder = TestProto2.HighIndexMessage.newBuilder();
    builder.addRepeatedInt(1);
    builder.addRepeatedInt(2);

    testData(false, builder.build());
  }

  @Test
  public void testProto3LargeProtobufferFieldId() throws Exception {
    TestProto3.HighIndexMessage.Builder builder = TestProto3.HighIndexMessage.newBuilder();
    builder.addRepeatedInt(1);
    builder.addRepeatedInt(2);

    testData(true, builder.build());
  }
}
