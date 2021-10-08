/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cdc.connector.sample.pubsub;

import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;

public class PercentileMessageReceiver implements MessageReceiver {

  private long recordCount;
  private long recordTotalCommittedToPublished;
  private long recordTotalCommittedToEmitted;
  private final List<Long> committedToPublishedMillis;
  private final List<Long> committedToEmittedMillis;
  private final OutputFile outputFile;

  public PercentileMessageReceiver(int initialCapacity, OutputFile outputFile) {
    this.recordCount = 0L;
    this.recordTotalCommittedToPublished = 0L;
    this.committedToPublishedMillis = new ArrayList<>(initialCapacity);
    this.committedToEmittedMillis = new ArrayList<>(initialCapacity);
    this.outputFile = outputFile;
  }

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    recordCount++;
    final String data = message.getData().toString(Charsets.UTF_8);
    final Timestamp publishTimestamp = Timestamp.fromProto(message.getPublishTime());
    final Timestamp now = Timestamp.now();
    try {
      final String[] fields = data.split(",");
      final String partitionToken = fields[0];
      final Timestamp commitTimestamp = Timestamp.parseTimestamp(fields[1]);
      final Timestamp emittedTimestamp = Timestamp.parseTimestamp(fields[2]);

      final long committedToPublished = TimestampConverter.millisBetween(commitTimestamp, publishTimestamp);
      final long committedToEmitted = TimestampConverter.millisBetween(commitTimestamp, emittedTimestamp);

      recordTotalCommittedToPublished += committedToPublished;
      recordTotalCommittedToEmitted += committedToEmitted;
      committedToPublishedMillis.add(committedToPublished);
      committedToEmittedMillis.add(committedToEmitted);
      outputFile.write(committedToPublished);

      if (recordCount % 10_000L == 0) {
        System.out.println("Average as of " + now);
        System.out.println("\tCommitted to published: " + (recordTotalCommittedToPublished / recordCount) + "ms");
        System.out.println("\tCommitted to emitted: " + (recordTotalCommittedToEmitted / recordCount) + "ms");
        System.out.println();
      }
    } catch (Exception e) {
      System.out.println("Error parsing message " + data);
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    consumer.ack();
  }

  public void printPercentiles() {
    System.out.println("Sorting committed to published array");
    final long start = System.currentTimeMillis();
    committedToPublishedMillis.sort(Long::compare);
    final long end = System.currentTimeMillis();
    System.out.println("Sorted committed to published array in " + (end - start) + "ms");

    final Timestamp now = Timestamp.now();
    System.out.println("Stats for " + now);
    System.out.println("\t" + recordCount + " data records processed");
    System.out.println("\tCommitted to published");
    System.out.println("\t\tMin             : " + committedToPublishedMillis.get(0));
    System.out.println("\t\tAverage         : " + ((double) recordTotalCommittedToPublished / recordCount));
    System.out.println("\t\t50th percentile : " + committedToPublishedMillis.get((int) (0.5 * committedToPublishedMillis.size())));
    System.out.println("\t\t90th percentile : " + committedToPublishedMillis.get((int) (0.9 * committedToPublishedMillis.size())));
    System.out.println("\t\t95th percentile : " + committedToPublishedMillis.get((int) (0.95 * committedToPublishedMillis.size())));
    System.out.println("\t\t99th percentile : " + committedToPublishedMillis.get((int) (0.99 * committedToPublishedMillis.size())));
    System.out.println("\t\tMax             : " + committedToPublishedMillis.get(committedToPublishedMillis.size() - 1));
    System.out.println();
    System.out.println("\tCommitted to emitted");
    System.out.println("\t\tMin             : " + committedToEmittedMillis.get(0));
    System.out.println("\t\tAverage         : " + ((double) recordTotalCommittedToEmitted / recordCount));
    System.out.println("\t\t50th percentile : " + committedToEmittedMillis.get((int) (0.5 * committedToEmittedMillis.size())));
    System.out.println("\t\t90th percentile : " + committedToEmittedMillis.get((int) (0.9 * committedToEmittedMillis.size())));
    System.out.println("\t\t95th percentile : " + committedToEmittedMillis.get((int) (0.95 * committedToEmittedMillis.size())));
    System.out.println("\t\t99th percentile : " + committedToEmittedMillis.get((int) (0.99 * committedToEmittedMillis.size())));
    System.out.println("\t\tMax             : " + committedToEmittedMillis.get(committedToEmittedMillis.size() - 1));
    System.out.println();
  }
}
