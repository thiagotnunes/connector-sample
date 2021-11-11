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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PercentileReader {

  public static void main(String[] args) throws IOException {
    final int initialCapacity = 40_000_000;
    final Map<String, List<Long>> keyToValues = new HashMap<>();
    keyToValues.put("CTR", new ArrayList<>(initialCapacity));
    keyToValues.put("RTE", new ArrayList<>(initialCapacity));
    keyToValues.put("CTE", new ArrayList<>(initialCapacity));
    keyToValues.put("CTP", new ArrayList<>(initialCapacity));
    keyToValues.put("PCS", new ArrayList<>(initialCapacity));
    keyToValues.put("PSR", new ArrayList<>(initialCapacity));
    keyToValues.put("SSTSE", new ArrayList<>(initialCapacity));
    System.out.println("Processing input");
    try (BufferedReader reader = new BufferedReader(new FileReader("output/output.txt"))) {
      String line = reader.readLine();
      while (line != null) {
        final String[] fields = line.split(" ");
        String key = fields[0];
        key = key.equals("COP") ? "CTP" : key;
        key = key.equals("COE") ? "CTE" : key;
        final long value = Long.parseLong(fields[1]);
        keyToValues.get(key).add(value);
        line = reader.readLine();
      }
    }
    System.out.println("Done processing input");
    System.out.println();

    final Timestamp now = Timestamp.now();
    System.out.println("Stats for " + now);
    System.out.println("\t" + keyToValues.get("CTP").size() + " data records processed");
    sortAndPrintStats(keyToValues.get("CTP"), "Committed to published");
    sortAndPrintStats(keyToValues.get("CTE"), "Committed to emitted");
    sortAndPrintStats(keyToValues.get("CTR"), "Committed to read");
    sortAndPrintStats(keyToValues.get("RTE"), "Read to emitted");
    sortAndPrintStats(keyToValues.get("SSTSE"), "Stream start to stream end");
    sortAndPrintStats(keyToValues.get("PCS"), "Partition created to scheduled");
    sortAndPrintStats(keyToValues.get("PSR"), "Partition scheduled to running");
  }

  private static void sortAndPrintStats(List<Long> values, String name) {
    values.sort(Long::compareTo);
    System.out.println("\t" + name);
    System.out.println("\t\tMin             : " + values.get(0));
    System.out.println("\t\t50th percentile : " + values.get((int) (0.5 * values.size())));
    System.out.println("\t\t90th percentile : " + values.get((int) (0.9 * values.size())));
    System.out.println("\t\t95th percentile : " + values.get((int) (0.95 * values.size())));
    System.out.println("\t\t99th percentile : " + values.get((int) (0.99 * values.size())));
    System.out.println("\t\tMax             : " + values.get(values.size() - 1));
    System.out.println();
  }

}
