package com.tsurugidb.iceaxe.test.low;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;
import com.tsurugidb.tsubakuro.sql.io.DateTimeInterval;

public class TestResultSet extends TestServerResource implements ResultSet {

    @Override
    public boolean nextRow() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public boolean nextColumn() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public boolean isNull() {
        throw new AssertionError("do override");
    }

    @Override
    public boolean fetchBooleanValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public int fetchInt4Value() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public long fetchInt8Value() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public float fetchFloat4Value() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public double fetchFloat8Value() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public BigDecimal fetchDecimalValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public String fetchCharacterValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public byte[] fetchOctetValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public boolean[] fetchBitValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public LocalDate fetchDateValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public LocalTime fetchTimeOfDayValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public LocalDateTime fetchTimePointValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public OffsetTime fetchTimeOfDayWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public OffsetDateTime fetchTimePointWithTimeZoneValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public DateTimeInterval fetchDateTimeIntervalValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public int beginArrayValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public void endArrayValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public int beginRowValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public void endRowValue() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }

    @Override
    public ResultSetMetadata getMetadata() throws IOException, ServerException, InterruptedException {
        throw new AssertionError("do override");
    }
}
