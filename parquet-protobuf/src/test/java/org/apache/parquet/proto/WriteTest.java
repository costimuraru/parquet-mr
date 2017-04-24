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

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.proto.test.MapProtobuf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class WriteTest {
  public static void main(String[] args) throws IOException {

    List<MessageOrBuilder> messages = Lists.newArrayList();
    for (int i = 0; i < 1; i++) {

      MapProtobuf.Log message = MapProtobuf.Log.newBuilder()
        .setIp("ip")
        .setTimestamp("timestamp")
        .setMessage("message")
        .putAdditional("one-key", "value")
        .addMyArray("array_value_0")
        .build();

      messages.add(message);
    }

    try {
      Files.delete(Paths.get("/tmp/test20.parquet"));
    } catch(Exception e) {}

    Path outputPath = new Path("/tmp/test20.parquet");
    writeMessages(MapProtobuf.Log.class, outputPath, messages.toArray(new MessageOrBuilder[messages.size()]));

  }

  public static void writeMessages(Class<? extends Message> cls, Path file,
                                   MessageOrBuilder... records) throws IOException {

    ProtoParquetWriter<MessageOrBuilder> writer = new ProtoParquetWriter<MessageOrBuilder>(file, cls);

    try {
      for (MessageOrBuilder record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }
}
