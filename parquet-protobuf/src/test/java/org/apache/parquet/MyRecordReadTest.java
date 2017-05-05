package org.apache.parquet;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

import static com.adobe.aam.proto.v3.DataCollection.DataCollectionMessage;

public class MyRecordReadTest {

  public static void main(String[] args) throws IOException {
    String path = "/Users/cmuraru/Dropbox/parquet/local2-protobuf-hadoop_log.1493990250-JgQSb.tmp";
    InputStream inputStream = new FileInputStream(path);
    DataCollectionMessage dataCollection = DataCollectionMessage.parseDelimitedFrom(inputStream);
    System.out.println(dataCollection);

    DataCollectionMessage.Builder dataCollection2 = DataCollectionMessage.newBuilder();

    dataCollection2.setSource(dataCollection.getSource());
//    dataCollection2.addAllRequestIds(dataCollection.getRequestIdsList());
//    dataCollection2.setPrimaryDeviceId(dataCollection.getPrimaryDeviceId());
//    dataCollection2.addAllDeclaredMappings(dataCollection.getDeclaredMappingsList());
//    dataCollection2.addAllTraits(dataCollection.getTraitsList());
//    dataCollection2.setTimestamps(dataCollection.getTimestamps());
//    dataCollection2.putAllDerivedSignals(dataCollection.getDerivedSignalsMap());
//    dataCollection2.setLocation(dataCollection.getLocation());
//    dataCollection2.setBillingInformation(dataCollection.getBillingInformation());
//    dataCollection2.setCostInformation(dataCollection.getCostInformation());
//    dataCollection2.setRequest(dataCollection.getRequest());


//    Map<String, String> headers = Maps.newHashMap();
//    headers.put("d1", "v1");
//    dataCollection2.setResponse(DataCollection.Response.newBuilder().putAllHeaders(headers));
//    dataCollection2.setSource(DataCollection.Source.newBuilder().setCaller("my_caller"));
//    dataCollection2.putDerivedSignals("key1", "value1");


    try {
      java.nio.file.Files.delete(Paths.get("/tmp/test.dcs.parquet"));
    } catch (Exception e) {
    }

    Path outputPath = new Path("/tmp/test.dcs.parquet");
    writeMessages(DataCollectionMessage.class, outputPath, dataCollection2);
  }



  public static void writeMessages(Class<? extends Message> cls, Path file,
                                   MessageOrBuilder record) throws IOException {

    ProtoParquetWriter<MessageOrBuilder> writer = new ProtoParquetWriter<MessageOrBuilder>(file, cls, CompressionCodecName.GZIP, 256 * 1024 * 1024, 1 * 1024 * 1024);

    try {
        writer.write(record);
    } finally {
      writer.close();
    }
  }
}
