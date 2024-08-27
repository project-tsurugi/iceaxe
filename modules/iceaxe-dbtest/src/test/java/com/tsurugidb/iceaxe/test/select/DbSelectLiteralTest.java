package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select literal test
 */
class DbSelectLiteralTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectLiteralTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        logInitEnd(LOG, info);
    }

    private static final String COLUMN = "c";

    @Test
    void nullLiteral() throws Exception {
        test("null", entity -> {
            assertNull(entity.getStringOrNull(COLUMN));
            assertNull(entity.getIntOrNull(COLUMN));
        });
    }

    @Test
    void longLiteral() throws Exception {
        long literal = 1;
        test(Long.toString(literal), entity -> {
            assertEquals(literal, entity.getLong(COLUMN));

            assertEquals((int) literal, entity.getInt(COLUMN));
            assertEquals(BigDecimal.valueOf(literal), entity.getDecimal(COLUMN));
        });
    }

    @Test
    void doubleLiteral() throws Exception {
        double literal = 12.3;
        test(Double.toString(literal), entity -> {
            assertEquals(literal, entity.getDouble(COLUMN));

            assertEquals(BigDecimal.valueOf(literal), entity.getDecimal(COLUMN));
        });
    }

    @Test
    void doubleLiteralE() throws Exception {
        test("1e2", entity -> {
            assertEquals(100d, entity.getDouble(COLUMN));
        });
    }

    @Test
    void stringLiteral() throws Exception {
        String literal = "abc";
        test(String.format("'%s'", literal), entity -> {
            assertEquals(literal, entity.getString(COLUMN));
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void booleanLiteral(boolean literal) throws Exception {
        test(Boolean.toString(literal), entity -> {
            assertEquals(literal, entity.getBoolean(COLUMN));
        });
    }

    @Test
    void dateLiteral() throws Exception {
        var literal = LocalDate.now();
        test(String.format("date'%s'", literal), entity -> {
            assertEquals(literal, entity.getDate(COLUMN));
        });
    }

    @Test
    void timeLiteral() throws Exception {
        var literal = LocalTime.now();
        test(String.format("time'%s'", literal), entity -> {
            assertEquals(literal, entity.getTime(COLUMN));
        });
    }

    @Test
    void dateTimeLiteral() throws Exception {
        var literal = LocalDateTime.now();
        test(String.format("timestamp'%s'", literal), entity -> {
            assertEquals(literal, entity.getDateTime(COLUMN));
        });
    }

    // TODO time with time zone literal

    @Test
    void offsetDateTimeLiteral() throws Exception {
        var literal = OffsetDateTime.now();
        test(String.format("timestamp with time zone'%s'", literal), entity -> {
            assertEquals(literal.withOffsetSameInstant(ZoneOffset.UTC), entity.getOffsetDateTime(COLUMN).withOffsetSameInstant(ZoneOffset.UTC));
        });
    }

    @Test
    void binaryLiteral() throws Exception {
        var literal = new byte[] { 0x12, (byte) 0xef };
        test("X'12ef'", entity -> {
            assertArrayEquals(literal, entity.getBytes(COLUMN));
        });
    }

    private static void test(String literal, Consumer<TsurugiResultEntity> assertion) throws IOException, InterruptedException {
        var sql = "select " + literal + " as " + COLUMN + " from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var entity = tm.executeAndFindRecord(ps).get();
            assertion.accept(entity);
        }
    }
}
