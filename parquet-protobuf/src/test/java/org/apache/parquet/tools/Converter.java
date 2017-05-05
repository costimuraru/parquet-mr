package org.apache.parquet.tools;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.security.SecureRandom;

public class Converter {
  private static SecureRandom random = new SecureRandom();

  public static void main(String[] args) throws IOException {

//    List<MessageOrBuilder> messages = Lists.newArrayList();
//    for (int i = 0; i < 10000; i++) {
//      String dpuuid = "dpuuid:" + new BigInteger(130, random).toString(32);
//
//      String signalKey1 = new BigInteger(130, random).toString(8);
//      String signalValue1 = new BigInteger(130, random).toString(8);
//
//      String dcsServer = new BigInteger(130, random).toString(3);
//
//      String debugId = new BigInteger(130, random).toString(32);
//
//      DataCollectionMessage message = DataCollectionMessage.newBuilder()
//        .setSource(DataCollection.Source.newBuilder().setComponent(DataCollection.Source.Component.DCS).setCaller("Analytics"))
//        .addRequestIds(0, Id.newBuilder().setId("device_id1").setNamespace(0))
//        .addRequestIds(1, Id.newBuilder().setId(dpuuid).setNamespace(300))
//        .setPrimaryDeviceId(Id.newBuilder().setId("device_id1").setNamespace(0))
//        .addTraits(DataCollection.Traits.newBuilder().addExistingTraits(AamCommon.Trait.newBuilder().setId(999).addFrequencies(AamCommon.Frequency.newBuilder().setDay(1000).setRealizations(7))))
//        .addDeclaredMappings(0, DataCollection.DeclaredMappings.newBuilder().setId(Id.newBuilder().setId("device_id1").setNamespace(0)).addMappedIds(0, Id.newBuilder().setId(dpuuid).setNamespace(300)))
//        .addMergeRules(0, DataCollection.MergeRule.newBuilder().setMergeRuleId(555)
//          .addSegments(0, Segment.newBuilder().setStatus(Segment.Status.NEW).setId(90000))
//          .addSegments(1, Segment.newBuilder().setStatus(Segment.Status.NEW).setId(90001))
//          .addSegments(2, Segment.newBuilder().setStatus(Segment.Status.NEW).setId(90002))
//          .addSegments(3, Segment.newBuilder().setStatus(Segment.Status.REACQUIRED).setId(90003))
//          .addResolvedId(0, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.CURRENT).setId(Id.newBuilder().setId("device_id1").setNamespace(0)))
//          .addResolvedId(1, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.CURRENT).setId(Id.newBuilder().setId(dpuuid).setNamespace(300)))
//          .addResolvedId(2, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.DEVICE_GRAPH).setId(Id.newBuilder().setId("device_id2").setNamespace(0)))
//          .addResolvedId(3, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.DEVICE_GRAPH).setId(Id.newBuilder().setId("device_id2").setNamespace(0)))
//        )
//        .addMergeRules(1, DataCollection.MergeRule.newBuilder().setMergeRuleId(777)
//          .addSegments(0, Segment.newBuilder().setStatus(Segment.Status.NEW).setId(80000))
//          .addSegments(1, Segment.newBuilder().setStatus(Segment.Status.REACQUIRED).setId(80003))
//          .addResolvedId(0, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.CURRENT).setId(Id.newBuilder().setId(dpuuid).setNamespace(300)))
//          .addResolvedId(1, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.LAST).setId(Id.newBuilder().setId("device_id2").setNamespace(0)))
//          .addResolvedId(2, DataCollection.ResolvedId.newBuilder().setFetchType(FetchType.LAST).setId(Id.newBuilder().setId("device_id2").setNamespace(0)))
//        )
//        .setTimestamps(AamCommon.Timestamps.newBuilder().setBusinessTimestamp(System.currentTimeMillis()).setProcessedTimestamp(System.currentTimeMillis()))
//        .setDeviceMetadata(DataCollection.DeviceMetadata.newBuilder().setIsRobot("false").setManufacturer("Apple").setMarketingName("Macbook").setModel("Pro").setOsName("OSX").setOsVersion("10.1").setPrimaryHardwareType("-"))
//        .setLocation(DataCollection.Location.newBuilder().setLocationId(300).setCountryCode("ro"))
//        .setBillingInformation(DataCollection.BillingInformation.newBuilder().setBillingCredits(1).setIsBillable(true).setBillingCredits(1.1))
//        .setCostInformation(DataCollection.CostInformation.newBuilder().setCostCredits(1.8).setCassandraReads(3).setTraitCreditsUsed(1982))
//        .setRequest(DataCollection.Request.newBuilder()
//          .putHeaders("Cookie", "demdex=somevalue")
//          .putHeaders("Method", "GET")
//          .setClientIp("89.102.27.50")
//          .setReferrerUrl("adobe.com")
//          .setUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393").setTargetUrl("/event").setSecure(true)
//        )
//        .setServer(DataCollection.Server.newBuilder().setHost("dcs1.usw2.demdex.net")
//          .setRegion(1)
//          .setVersion("master-5.3.3.20170118.165038"))
//        .setPartner(AamCommon.Partner.newBuilder().setPartnerId(1166).setContainerId(1).setDomainId(1).setSubdomain("adobe"))
//        .setResponse(DataCollection.Response.newBuilder()
//          .putHeaders("Set-Cookie", "demdex=value")
//          .putHeaders("X-TID", "203uhds09z=")
//          .putHeaders("X-Error", "1,2,3")
//          .setHttpCode(200)
//          .setResponseBody("{\"body\": \"response body here\"")
//        )
//        .addOptout(DataCollection.OptOut.newBuilder().setOptOutType(DataCollection.OptOut.Type.GLOBAL))
//        .setDebug(DataCollection.Debug.newBuilder().setDebugId(debugId)
//          .setTransactionId("203uhds09z=")
//        )
//        .build();
//
//      messages.add(message);
//    }
//
//    try {
//      Files.delete(Paths.get("/tmp/test.dcs.parquet"));
//    } catch (Exception e) {
//    }
//
//    Path outputPath = new Path("/tmp/test.dcs.parquet");
//    writeMessages(DataCollectionMessage.class, outputPath, messages.toArray(new MessageOrBuilder[messages.size()]));
  }

  public static void writeMessages(Class<? extends Message> cls, Path file,
                                   MessageOrBuilder... records) throws IOException {

    ProtoParquetWriter<MessageOrBuilder> writer = new ProtoParquetWriter<MessageOrBuilder>(file, cls, CompressionCodecName.GZIP, 256 * 1024 * 1024, 1 * 1024 * 1024);

    try {
      for (MessageOrBuilder record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }
}
