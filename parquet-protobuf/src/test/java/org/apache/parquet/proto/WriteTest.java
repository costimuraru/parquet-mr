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
    for (int i = 0; i < 3; i++) {

      MapProtobuf.MyMessage message = MapProtobuf.MyMessage.newBuilder()
        .setMyId(123)
        .putMyMap("somekey", 2.0)
        .build();

      messages.add(message);
    }

    try {
      Files.delete(Paths.get("/tmp/test20.parquet"));
    } catch(Exception e) {}

    Path outputPath = new Path("/tmp/test20.parquet");
    writeMessages(MapProtobuf.MyMessage.class, outputPath, messages.toArray(new MessageOrBuilder[messages.size()]));

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
