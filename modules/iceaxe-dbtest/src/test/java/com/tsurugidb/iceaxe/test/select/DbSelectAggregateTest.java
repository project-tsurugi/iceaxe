package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select aggregate test
 */
class DbSelectAggregateTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectAggregateTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by count(*)" })
    void selectCount(String order) throws IOException {
        var sql = "select count(*) from " + TEST + order;
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> ps.executeAndFindRecord(tm));
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                return;
            }
            int count = ps.executeAndFindRecord(tm).get();
            assertEquals(SIZE, count);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by sum(bar)" })
    void selectSum(String order) throws IOException {
        var sql = "select sum(bar) as sum, min(zzz) as zzz from " + TEST + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> ps.executeAndFindRecord(tm));
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                return;
            }
            TsurugiResultEntity entity = ps.executeAndFindRecord(tm).get();
            assertEquals(LongStream.range(0, SIZE).sum(), entity.getInt8OrNull("sum"));
            assertEquals("0", entity.getCharacterOrNull("zzz"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by foo" })
    void selectKeyCount(String order) throws IOException {
        var sql = "select foo, count(*) as cnt from " + TEST + " group by foo" + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> ps.executeAndGetList(tm));
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                return;
            }
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());

            int i = 0;
            for (var actual : list) {
                if (!order.isEmpty()) {
                    assertEquals(i, actual.getInt4("foo"));
                }
                assertEquals(1, actual.getInt4("cnt"));
                i++;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by foo" })
    void selectKeySum(String order) throws IOException {
        var sql = "select foo, sum(bar) as sum, min(zzz) as zzz from " + TEST + " group by foo" + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> ps.executeAndGetList(tm));
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                return;
            }
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());

            int i = 0;
            for (var actual : list) {
                int foo = actual.getInt4("foo");
                if (!order.isEmpty()) {
                    assertEquals(i, foo);
                }
                assertEquals(Long.valueOf(foo), actual.getInt8("sum"));
                assertEquals(Integer.toString(foo), actual.getCharacter("zzz"));
                i++;
            }
        }
    }

    @Test
    void selectCountWhereEmpty() throws IOException {
        new DbSelectEmptyTest().selectCount(" where foo<0");
    }

    @Test
    void selectSumWhereEmpty() throws IOException {
        new DbSelectEmptyTest().selectSum(" where foo<0");
    }

    @Test
    void selectKeyCountWhereEmpty() throws IOException {
        new DbSelectEmptyTest().selectKeyCount(" where foo<0");
    }

    @Test
    void selectKeySumWhereEmpty() throws IOException {
        new DbSelectEmptyTest().selectKeySum(" where foo<0");
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo,foo", "foo,foo,foo" })
    @Disabled // TODO remove Disabled group byに同カラムが複数あるとtateyama-serverがクラッシュする
    void selectGroupBySameKey(String groupKey) throws IOException {
        var sql = "select foo, count(*) as cnt from " + TEST + " group by " + groupKey;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());

            for (var actual : list) {
                assertEquals(1, actual.getInt4("cnt"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo", "foo,foo" })
    void selectKeyWithoutGroup(String key) throws IOException {
        var sql = "select " + key + ", count(*) from " + TEST; // without 'group by'

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of())) {
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                ps.executeAndGetList(tm, TgParameterList.of());
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            // TODO 複数エラーがある場合のエラーメッセージ結合方法
            int size = key.split(",").length;
            String expected = "translating statement failed: " + Stream.generate(() -> "invalid_aggregation_column target column must be aggregated").limit(size).collect(Collectors.joining());
            assertContains(expected, e.getMessage());
        }
    }
}
