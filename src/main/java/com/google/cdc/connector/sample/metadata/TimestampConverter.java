/*
 * Copyright 2022 Google LLC
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

package com.google.cdc.connector.sample.metadata;

import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimestampConverter {

  public static void main(String[] args) {
    final String utc = "2022-04-19T07:38:43.485000000Z";
    System.out.println(ZonedDateTime
        .parse(utc)
        .withZoneSameInstant(ZoneId.of("Australia/Sydney")));
    System.out.println(ZonedDateTime
        .parse(utc)
        .withZoneSameInstant(ZoneId.of("America/Los_Angeles")));
  }

}
