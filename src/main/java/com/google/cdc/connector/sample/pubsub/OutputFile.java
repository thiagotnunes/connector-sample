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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class OutputFile {

  private final String fileName;
  private final boolean append;
  private BufferedWriter writer;

  public OutputFile(String fileName, boolean append) {
    this.fileName = fileName;
    this.append = append;
  }

  public void open() throws IOException {
    writer = new BufferedWriter(new FileWriter(fileName, append));
  }

  public void write(long millis) throws IOException {
    try {
      writer.write(millis + "\n");
      writer.flush();
    } catch (Exception e) {
      System.err.println("ERROR Writing file " + e);
      e.printStackTrace();
      writer.close();
    }
  }

  public void close() throws IOException {
    writer.close();
  }
}
