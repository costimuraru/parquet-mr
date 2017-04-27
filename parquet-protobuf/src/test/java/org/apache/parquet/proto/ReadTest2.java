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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.test.ListOfListOuterClass;
import org.apache.parquet.proto.test.ListOfMessageOuterClass;
import org.apache.parquet.proto.test.SimpleListOuterClass.SimpleList;
import org.apache.parquet.proto.test.SimpleListOuterClass.SimpleListOrBuilder;

import java.io.IOException;

public class ReadTest2 {

  public static void main1(String[] args) throws IOException {

    Path outputPath = new Path("/tmp/test24.parquet");

    ParquetReader<SimpleList> reader = ProtoParquetReader.<SimpleList>builder(outputPath).build();
    SimpleListOrBuilder log = reader.read();
    System.out.println(log);
  }

  public static void main(String[] args) throws IOException {

    Path outputPath = new Path("/tmp/test23.parquet");

    ParquetReader<ListOfListOuterClass.ListOfList> reader = ProtoParquetReader.<ListOfListOuterClass.ListOfList>builder(outputPath).build();
    ListOfListOuterClass.ListOfListOrBuilder log = reader.read();
    System.out.println(log);
  }

  public static void main2(String[] args) throws IOException {

    Path outputPath = new Path("/tmp/test25.parquet");

    ParquetReader<ListOfMessageOuterClass.ListOfMessage> reader = ProtoParquetReader.<ListOfMessageOuterClass.ListOfMessage>builder(outputPath).build();
    Object log = reader.read();
    System.out.println(log);
  }
}
