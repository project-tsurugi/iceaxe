package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select literal test
 */
class DbSelectLiteralTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectLiteralTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        LOG.debug("init end");
    }

    private static final String COLUMN = "c";

    @Test
    void nullLiteral() throws IOException {
        test("null", entity -> {
            assertNull(entity.getStringOrNull(COLUMN));
            assertNull(entity.getIntOrNull(COLUMN));
        });
    }

    @Test
    void longLiteral() throws IOException {
        long literal = 1;
        test(Long.toString(literal), entity -> {
            assertEquals(literal, entity.getLong(COLUMN));

            assertEquals((int) literal, entity.getInt(COLUMN));
            assertEquals(BigDecimal.valueOf(literal), entity.getDecimal(COLUMN));
        });
    }

    @Test
    void doubleLiteral() throws IOException {
        double literal = 12.3;
        test(Double.toString(literal), entity -> {
            assertEquals(literal, entity.getDouble(COLUMN));

            assertEquals(BigDecimal.valueOf(literal), entity.getDecimal(COLUMN));
        });
    }

    @Test
    void doubleLiteralError() throws IOException {
        var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
            test("1e2", entity -> {
            });
        });
        assertEqualsCode(SqlServiceCode.ERR_PARSE_ERROR, e);
        assertContains("error in db_->create_executable()", e.getMessage()); // TODO エラー詳細情報の確認
    }

    @Test
    void stringLiteral() throws IOException {
        String literal = "abc";
        test(String.format("'%s'", literal), entity -> {
            assertEquals(literal, entity.getString(COLUMN));
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void booleanLiteral(boolean literal) throws IOException {
        test(Boolean.toString(literal), entity -> {
            assertEquals(literal, entity.getBoolean(COLUMN));
        });
    }

    // TODO date literal

    private static void test(String literal, Consumer<TsurugiResultEntity> assertion) throws IOException {
        var sql = "select " + literal + " as " + COLUMN + " from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var entity = tm.executeAndFindRecord(ps).get();
            assertion.accept(entity);
        }
    }
}
