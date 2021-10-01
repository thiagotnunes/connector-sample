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

import static com.google.cdc.connector.sample.pubsub.TimestampConverter.millisBetween;

import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import java.text.NumberFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;

public class BucketedMessageReceiver implements MessageReceiver {
    private long recordCount = 0L;
    private long recordCommittedToPublished_0ms_to_100ms = 0L;
    private long recordCommittedToPublished_100ms_to_300ms = 0L;
    private long recordCommittedToPublished_300ms_to_500ms = 0L;
    private long recordCommittedToPublished_500ms_to_1000ms = 0L;
    private long recordCommittedToPublished_1000ms_to_3000ms = 0L;
    private long recordCommittedToPublished_3000ms_to_inf = 0L;
    private long recordTotalCommittedToPublished = 0L;
    private long recordMinCommittedToPublished = Long.MAX_VALUE;
    private long recordMaxCommittedToPublished = Long.MIN_VALUE;

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
      final String data = message.getData().toString(Charsets.UTF_8);
      final Timestamp publishTimestamp = Timestamp.fromProto(message.getPublishTime());
      try {
        final String[] fields = data.split(",");
        final String partitionToken = fields[0];
        final Timestamp commitTimestamp = Timestamp.parseTimestamp(fields[1]);
        final Timestamp now = Timestamp.now();

        final long committedToPublishedMs = millisBetween(commitTimestamp, publishTimestamp);

        recordCount++;
        recordTotalCommittedToPublished += committedToPublishedMs;
        if (committedToPublishedMs < recordMinCommittedToPublished) {
          recordMinCommittedToPublished = committedToPublishedMs;
        }
        if (committedToPublishedMs > recordMaxCommittedToPublished) {
          recordMaxCommittedToPublished = committedToPublishedMs;
        }
        if (committedToPublishedMs < 100) {
          recordCommittedToPublished_0ms_to_100ms++;
        } else if (committedToPublishedMs < 300) {
          recordCommittedToPublished_100ms_to_300ms++;
        } else if (committedToPublishedMs < 500) {
          recordCommittedToPublished_300ms_to_500ms++;
        } else if (committedToPublishedMs < 1000) {
          recordCommittedToPublished_500ms_to_1000ms++;
        } else if (committedToPublishedMs < 3000) {
          recordCommittedToPublished_1000ms_to_3000ms++;
        } else {
          recordCommittedToPublished_3000ms_to_inf++;
        }

        if (recordCount % 25_000L == 0) {
          final NumberFormat format = NumberFormat.getPercentInstance();
          format.setMinimumFractionDigits(2);
          format.setMaximumFractionDigits(2);
          System.out.println();
          System.out.println("Stats for " + now);
          System.out.println("\t" + recordCount + " data records processed");
          System.out.println("\tCommitted to published");
          System.out.println("\t\tMin            : " + recordMinCommittedToPublished);
          System.out.println("\t\tAverage        : " + ((double) recordTotalCommittedToPublished / recordCount));
          System.out.println("\t\tMax            : " + recordMaxCommittedToPublished);
          System.out.println("\t\t0ms    -  100ms: " + format.format((double) recordCommittedToPublished_0ms_to_100ms
              / recordCount) + " (" + recordCommittedToPublished_0ms_to_100ms + ")");
          System.out.println("\t\t100ms  -  300ms: " + format.format((double) recordCommittedToPublished_100ms_to_300ms
              / recordCount) + " (" + recordCommittedToPublished_100ms_to_300ms + ")");
          System.out.println("\t\t300ms  -  500ms: " + format.format((double) recordCommittedToPublished_300ms_to_500ms
              / recordCount) + " (" + recordCommittedToPublished_300ms_to_500ms + ")");
          System.out.println("\t\t500ms  - 1000ms: " + format.format((double) recordCommittedToPublished_500ms_to_1000ms
              / recordCount) + " (" + recordCommittedToPublished_500ms_to_1000ms + ")");
          System.out.println("\t\t1000ms - 3000ms: " + format.format((double) recordCommittedToPublished_1000ms_to_3000ms
              / recordCount) + " (" + recordCommittedToPublished_1000ms_to_3000ms + ")");
          System.out.println("\t\t3000ms -    inf: " + format.format((double) recordCommittedToPublished_3000ms_to_inf
              / recordCount) + " (" + recordCommittedToPublished_3000ms_to_inf + ")");
          System.out.println();
        }

      } catch (Exception e) {
        System.out.println("Error parsing message " + data);
      }
      consumer.ack();
    }
}
