/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe.sql.result.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;

class TgSingleResultMappingTest {

    @Test
    void testOfBoolean() {
        assertMapping(true, TgSingleResultMapping.ofBoolean());
    }

    @Test
    void testOfInt() {
        assertMapping(123, TgSingleResultMapping.ofInt());
    }

    @Test
    void testOfLong() {
        assertMapping(123L, TgSingleResultMapping.ofLong());
    }

    @Test
    void testOfFloat() {
        assertMapping(123f, TgSingleResultMapping.ofFloat());
    }

    @Test
    void testOfDouble() {
        assertMapping(123d, TgSingleResultMapping.ofDouble());
    }

    @Test
    void testOfDecimal() {
        assertMapping(BigDecimal.valueOf(123), TgSingleResultMapping.ofDecimal());
    }

    @Test
    void testOfString() {
        assertMapping("abc", TgSingleResultMapping.ofString());
    }

    @Test
    void testOfBytes() {
        assertMapping(new byte[] { 1, 2, 3 }, TgSingleResultMapping.ofBytes());
    }

    @Test
    void testOfBits() {
        assertMapping(new boolean[] { true, false, true }, TgSingleResultMapping.ofBits());
    }

    @Test
    void testOfDate() {
        assertMapping(LocalDate.of(2023, 3, 17), TgSingleResultMapping.ofDate());
    }

    @Test
    void testOfTime() {
        assertMapping(LocalTime.of(23, 59, 59), TgSingleResultMapping.ofTime());
    }

    @Test
    void testOfDateTime() {
        assertMapping(LocalDateTime.of(2023, 3, 17, 23, 59, 59), TgSingleResultMapping.ofDateTime());
    }

    @Test
    void testOfOffsetTime() {
        assertMapping(OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHours(9)), TgSingleResultMapping.ofOffsetTime());
    }

    @Test
    void testOfOffsetDateTime() {
        assertMapping(OffsetDateTime.of(2023, 3, 17, 23, 59, 59, 0, ZoneOffset.ofHours(9)), TgSingleResultMapping.ofOffsetDateTime());
    }

    @Test
    void testOfZonedDateTime() {
        var value = OffsetDateTime.of(2023, 3, 17, 23, 59, 59, 0, ZoneOffset.ofHours(9));
        var zone = ZoneId.of("Asia/Tokyo");
        var expected = ZonedDateTime.of(2023, 3, 17, 23, 59, 59, 0, zone);
        assertMapping(value, expected, TgSingleResultMapping.ofZonedDateTime(zone));
    }

    @Test
    void testOfClass() {
        assertMapping(123, TgSingleResultMapping.of(int.class));
        assertMapping(123L, TgSingleResultMapping.of(long.class));
    }

    @Test
    void testOfDataType() {
        assertMapping(123, TgSingleResultMapping.of(TgDataType.INT));
        assertMapping(123L, TgSingleResultMapping.of(TgDataType.LONG));
    }

    private static void assertMapping(Object value, TgSingleResultMapping<?> actual) {
        assertMapping(value, value, actual);
    }

    private static void assertMapping(Object value, Object expectedValue, TgSingleResultMapping<?> actual) {
        var type = TgDataType.of(value.getClass());
        var list = new ArrayList<>();
        list.add(value);
        var lowResultSet = new ResultSet() {
            private int index = -1;

            @Override
            public boolean nextRow() throws IOException, ServerException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean nextColumn() throws IOException, ServerException, InterruptedException {
                return ++index < list.size();
            }

            private Object getValue() {
                return list.get(index);
            }

            @Override
            public boolean isNull() {
                return getValue() == null;
            }

            @Override
            public boolean fetchBooleanValue() throws IOException, ServerException, InterruptedException {
                return (Boolean) getValue();
            }

            @Override
            public int fetchInt4Value() throws IOException, ServerException, InterruptedException {
                return (Integer) getValue();
            }

            @Override
            public long fetchInt8Value() throws IOException, ServerException, InterruptedException {
                return (Long) getValue();
            }

            @Override
            public float fetchFloat4Value() throws IOException, ServerException, InterruptedException {
                return (Float) getValue();
            }

            @Override
            public double fetchFloat8Value() throws IOException, ServerException, InterruptedException {
                return (Double) getValue();
            }

            @Override
            public BigDecimal fetchDecimalValue() throws IOException, ServerException, InterruptedException {
                return (BigDecimal) getValue();
            }

            @Override
            public String fetchCharacterValue() throws IOException, ServerException, InterruptedException {
                return (String) getValue();
            }

            @Override
            public byte[] fetchOctetValue() throws IOException, ServerException, InterruptedException {
                return (byte[]) getValue();
            }

            @Override
            public boolean[] fetchBitValue() throws IOException, ServerException, InterruptedException {
                return (boolean[]) getValue();
            }

            @Override
            public LocalDate fetchDateValue() throws IOException, ServerException, InterruptedException {
                return (LocalDate) getValue();
            }

            @Override
            public LocalTime fetchTimeOfDayValue() throws IOException, ServerException, InterruptedException {
                return (LocalTime) getValue();
            }

            @Override
            public LocalDateTime fetchTimePointValue() throws IOException, ServerException, InterruptedException {
                return (LocalDateTime) getValue();
            }

            @Override
            public OffsetTime fetchTimeOfDayWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
                return (OffsetTime) getValue();
            }

            @Override
            public OffsetDateTime fetchTimePointWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
                return (OffsetDateTime) getValue();
            }

            @Override
            public DateTimeInterval fetchDateTimeIntervalValue() throws IOException, ServerException, InterruptedException {
                return (DateTimeInterval) getValue();
            }

            @Override
            public int beginArrayValue() throws IOException, ServerException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void endArrayValue() throws IOException, ServerException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int beginRowValue() throws IOException, ServerException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public void endRowValue() throws IOException, ServerException, InterruptedException {
                throw new UnsupportedOperationException();
            }

            @Override
            public ResultSetMetadata getMetadata() throws IOException, ServerException, InterruptedException {
                return new ResultSetMetadata() {
                    @Override
                    public List<? extends Column> getColumns() {
                        return List.of(Column.newBuilder().setAtomType(type.getLowDataType()).build());
                    }
                };
            }
        };
        var record = new TsurugiResultRecord(null, lowResultSet, IceaxeConvertUtil.INSTANCE) {
        };
        try {
            assertEquals(expectedValue, actual.convert(record));
        } catch (IOException | InterruptedException | TsurugiTransactionException e) {
            throw new RuntimeException(e);
        }
    }
}
