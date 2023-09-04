package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select aggregate test
 */
class DbSelectAggregateTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectAggregateTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by count(*)" })
    void selectCount(String order) throws Exception {
        var sql = "select count(*) from " + TEST + order;
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndFindRecord(ps));
                assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
                return;
            }
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(SIZE, count);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by sum(bar)" })
    void selectSum(String order) throws Exception {
        var sql = "select sum(bar) as sum, min(zzz) as zzz from " + TEST + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndFindRecord(ps));
                assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
                return;
            }
            TsurugiResultEntity entity = tm.executeAndFindRecord(ps).get();
            assertEquals(LongStream.range(0, SIZE).sum(), entity.getLongOrNull("sum"));
            assertEquals("0", entity.getStringOrNull("zzz"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by foo" })
    void selectKeyCount(String order) throws Exception {
        var sql = "select foo, count(*) as cnt from " + TEST + " group by foo" + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetList(ps));
                assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
                return;
            }
            var list = tm.executeAndGetList(ps);
            assertEquals(SIZE, list.size());

            int i = 0;
            for (var actual : list) {
                if (!order.isEmpty()) {
                    assertEquals(i, actual.getInt("foo"));
                }
                assertEquals(1, actual.getInt("cnt"));
                i++;
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by foo" })
    void selectKeySum(String order) throws Exception {
        var sql = "select foo, sum(bar) as sum, min(zzz) as zzz from " + TEST + " group by foo" + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            if (order.contains("order by")) { // TODO 集約のorder by実装待ち
                var e = assertThrowsExactly(TsurugiTmIOException.class, () -> tm.executeAndGetList(ps));
                assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
                return;
            }
            var list = tm.executeAndGetList(ps);
            assertEquals(SIZE, list.size());

            int i = 0;
            for (var actual : list) {
                int foo = actual.getInt("foo");
                if (!order.isEmpty()) {
                    assertEquals(i, foo);
                }
                assertEquals(Long.valueOf(foo), actual.getLong("sum"));
                assertEquals(Integer.toString(foo), actual.getString("zzz"));
                i++;
            }
        }
    }

    @Test
    void selectCountWhereEmpty() throws Exception {
        new DbSelectEmptyTest().selectCount(" where foo<0");
    }

    @Test
    void selectSumWhereEmpty() throws Exception {
        new DbSelectEmptyTest().selectSum(" where foo<0");
    }

    @Test
    void selectKeyCountWhereEmpty() throws Exception {
        new DbSelectEmptyTest().selectKeyCount(" where foo<0");
    }

    @Test
    void selectKeySumWhereEmpty() throws Exception {
        new DbSelectEmptyTest().selectKeySum(" where foo<0");
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo,foo", "foo,foo,foo" })
    @Disabled // TODO remove Disabled. group byに同カラムが複数あるとtateyama-serverがクラッシュする
    void selectGroupBySameKey(String groupKey) throws Exception {
        var sql = "select foo, count(*) as cnt from " + TEST + " group by " + groupKey;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(SIZE, list.size());

            for (var actual : list) {
                assertEquals(1, actual.getInt("cnt"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "foo", "foo,foo" })
    void selectKeyWithoutGroup(String key) throws Exception {
        var sql = "select " + key + ", count(*) from " + TEST; // without 'group by'

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, TgParameterMapping.of())) {
            var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                tm.executeAndGetList(ps, TgBindParameters.of());
            });
            assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
            // TODO 複数エラーがある場合のエラーメッセージ結合方法
            int size = key.split(",").length;
            String expected = "translating statement failed: " + Stream.generate(() -> "invalid_aggregation_column target column must be aggregated").limit(size).collect(Collectors.joining());
            assertContains(expected, e.getMessage());
        }
    }

    @Test
    void errorGroupNameNotFound() throws Exception {
        var sql = "select foo as k, count(*) as cnt from " + TEST + " group by k";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
//TODO      assertContains("translating statement failed: variable_not_found k)", e.getMessage());
        }
    }
}
