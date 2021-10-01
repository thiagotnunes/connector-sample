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
import java.math.BigDecimal;
import java.math.RoundingMode;

public class TimestampConverter {
  public static long millisBetween(Timestamp start, Timestamp end) {
    final BigDecimal startNanos = BigDecimal
        .valueOf(start.getSeconds())
        .scaleByPowerOfTen(9)
        .add(BigDecimal.valueOf(start.getNanos()));
    final BigDecimal endNanos = BigDecimal
        .valueOf(end.getSeconds())
        .scaleByPowerOfTen(9)
        .add(BigDecimal.valueOf(end.getNanos()));

    return endNanos
        .subtract(startNanos)
        .divide(BigDecimal.valueOf(1_000_000L), RoundingMode.CEILING)
        .longValue();
  }

}
