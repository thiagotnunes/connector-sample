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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PercentileReader {

  public static void main(String[] args) throws IOException {
    final int initialCapacity = 100_000_000;
    final List<Long> committedToEmittedMillis = new ArrayList<>(initialCapacity);
    final List<Long> committedToPublishedMillis = new ArrayList<>(initialCapacity);
    try (BufferedReader reader = new BufferedReader(new FileReader("output/output.txt"))) {
      String line = reader.readLine();
      while (line != null) {
        final String[] fields = line.split(" ");
        if (fields[0].equals("COE")) {
          committedToEmittedMillis.add(Long.parseLong(fields[1]));
        } else if (fields[0].equals("COP")){
          committedToPublishedMillis.add(Long.parseLong(fields[1]));
        } else {
          System.err.println("Unrecognized line " + line);
        }
        line = reader.readLine();
      }
    }

    System.out.println("Sorting arrays");
    committedToPublishedMillis.sort(Long::compare);
    committedToEmittedMillis.sort(Long::compare);
    System.out.println("Arrays sorted");
    final Timestamp now = Timestamp.now();
    System.out.println("Stats for " + now);
    System.out.println("\t" + committedToPublishedMillis.size() + " data records processed");
    System.out.println("\tCommitted to published");
    System.out.println("\t\tMin             : " + committedToPublishedMillis.get(0));
    System.out.println("\t\t50th percentile : " + committedToPublishedMillis.get((int) (0.5 * committedToPublishedMillis.size())));
    System.out.println("\t\t90th percentile : " + committedToPublishedMillis.get((int) (0.9 * committedToPublishedMillis.size())));
    System.out.println("\t\t95th percentile : " + committedToPublishedMillis.get((int) (0.95 * committedToPublishedMillis.size())));
    System.out.println("\t\t99th percentile : " + committedToPublishedMillis.get((int) (0.99 * committedToPublishedMillis.size())));
    System.out.println("\t\tMax             : " + committedToPublishedMillis.get(committedToPublishedMillis.size() - 1));
    System.out.println();
    System.out.println("\tCommitted to emitted");
    System.out.println("\t\tMin             : " + committedToEmittedMillis.get(0));
    System.out.println("\t\t50th percentile : " + committedToEmittedMillis.get((int) (0.5 * committedToEmittedMillis.size())));
    System.out.println("\t\t90th percentile : " + committedToEmittedMillis.get((int) (0.9 * committedToEmittedMillis.size())));
    System.out.println("\t\t95th percentile : " + committedToEmittedMillis.get((int) (0.95 * committedToEmittedMillis.size())));
    System.out.println("\t\t99th percentile : " + committedToEmittedMillis.get((int) (0.99 * committedToEmittedMillis.size())));
    System.out.println("\t\tMax             : " + committedToEmittedMillis.get(committedToEmittedMillis.size() - 1));
    System.out.println();
  }

}
