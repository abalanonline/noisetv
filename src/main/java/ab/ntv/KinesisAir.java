/*
 * Copyright 2022 Aleksei Balan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ab.ntv;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.lang.IllegalStateException;
import java.util.UUID;
import java.util.function.Consumer;

public class KinesisAir implements Atmosphere {
  private final String topic;

  private static StreamDescriptionSummary describeStream(KinesisClient client, String topic) {
    DescribeStreamSummaryRequest describeStreamSummaryRequest =
        DescribeStreamSummaryRequest.builder().streamName(topic).build();
    return client.describeStreamSummary(describeStreamSummaryRequest).streamDescriptionSummary();
  }

  public KinesisAir(String topic) {
    this.topic = topic;

    KinesisClient kinesisClient = KinesisClient.builder().build();
    try {
      describeStream(kinesisClient, topic);
    } catch (ResourceNotFoundException resourceNotFoundException) {
      CreateStreamRequest streamReq = CreateStreamRequest.builder()
          .streamName(topic)
          .shardCount(1)
          .build();
      kinesisClient.createStream(streamReq);
      do {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } while (describeStream(kinesisClient, topic).streamStatus() == StreamStatus.CREATING);
    }
  }

  @Override
  public Transmitter installTransmitter() {
    return new KinesisTransmitter(KinesisClient.builder().build(), topic);
  }

  @Override
  public Receiver installReceiver() {
    return new KinesisReceiver(KinesisClient.builder().build(), topic);
  }

  public static class KinesisTransmitter implements Transmitter {
    private final KinesisClient client;
    private final String topic;

    public KinesisTransmitter(KinesisClient client, String topic) {
      this.client = client;
      this.topic = topic;
    }

    @Override
    public void send(String message) {
      PutRecordRequest request = PutRecordRequest.builder()
          .partitionKey(UUID.randomUUID().toString())
          .streamName(topic)
          .data(SdkBytes.fromUtf8String(message))
          .build();
      client.putRecord(request);
    }
  }

  public static class KinesisReceiver implements Receiver {
    private final KinesisClient client;
    private final String topic;
    private final String shardIterator;

    public KinesisReceiver(KinesisClient client, String topic) {
      this.client = client;
      this.topic = topic;
      ListShardsResponse listShardsResponse =
          this.client.listShards(ListShardsRequest.builder().streamName(topic).build());
      Shard shard = listShardsResponse.shards().get(0); // FIXME: 2022-02-13 multiple shards
      GetShardIteratorRequest getShardIteratorRequest = GetShardIteratorRequest.builder()
          .streamName(topic)
          .shardId(shard.shardId())
          .shardIteratorType(ShardIteratorType.LATEST)
          .build();

      GetShardIteratorResponse getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
      this.shardIterator = getShardIteratorResult.shardIterator();
    }

    @Override
    public String receive() {
      throw new IllegalStateException("no sync");
    }

    @Override
    public void setListener(Consumer<String> listener) {
      new Thread(() -> {
        String shardIterator = this.shardIterator;
        while (true) {
          GetRecordsRequest getRecordsRequest = GetRecordsRequest.builder()
              .shardIterator(shardIterator)
              .limit(25)
              .build();
          GetRecordsResponse result = client.getRecords(getRecordsRequest);

          for (Record record : result.records()) {
            listener.accept(record.data().asUtf8String());
          }

          try {
            Thread.sleep(1000);
          } catch (InterruptedException exception) {
            throw new RuntimeException(exception);
          }

          shardIterator = result.nextShardIterator();
        }
      }).start();
    }
  }
}
