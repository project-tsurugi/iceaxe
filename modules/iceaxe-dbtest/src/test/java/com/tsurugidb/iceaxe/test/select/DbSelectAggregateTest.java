package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
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
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select aggregate test
 */
class DbSelectAggregateTest extends DbTestTableTester {

    private static final List<TestEntity> LIST = List.of( //
            new TestEntity(1, 2, "a"), //
            new TestEntity(2, 4, "b"), //
            new TestEntity(3, 8, "b"), //
            new TestEntity(4, 16, "c"), //
            new TestEntity(5, 32, "c"), //
            new TestEntity(6, 64, "d"), //
            new TestEntity(7, 128, "d"), //
            new TestEntity(8, 256, "d"));
    private static final Map<String, List<TestEntity>> ZZZ_MAP = LIST.stream().collect(Collectors.groupingBy(TestEntity::getZzz));

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectAggregateTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        for (var entity : LIST) {
            insertTestTable(entity);
        }

        logInitEnd(LOG, info);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by count(*)" })
    void selectCount(String order) throws Exception {
        var sql = "select count(*) from " + TEST + order;
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var tm = createTransactionManagerOcc(getSession());
        int count = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LIST.size(), count);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by sum(bar)" })
    void selectSum(String order) throws Exception {
        var sql = "select sum(bar) as bar, min(zzz) as zzz from " + TEST + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            TsurugiResultEntity entity = tm.executeAndFindRecord(ps).get();
            assertEquals(LIST.stream().mapToLong(TestEntity::getBar).sum(), entity.getLong("bar"));
            assertEquals(LIST.stream().map(TestEntity::getZzz).min(String::compareTo).get(), entity.getString("zzz"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by zzz" })
    void selectKeyCount(String order) throws Exception {
        var sql = "select zzz, count(*) as cnt from " + TEST + " group by zzz" + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(ZZZ_MAP.size(), list.size());

            String prev = "";
            for (var actual : list) {
                String zzz = actual.getString("zzz");
                if (!order.isEmpty()) {
                    assertTrue(zzz.compareTo(prev) > 0);
                    prev = zzz;
                }
                assertEquals(ZZZ_MAP.get(zzz).size(), actual.getInt("cnt"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by zzz" })
    void selectKeySum(String order) throws Exception {
        var sql = "select zzz, sum(bar) as bar, min(foo) as foo from " + TEST + " group by zzz" + order;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(ZZZ_MAP.size(), list.size());

            String prev = "";
            for (var actual : list) {
                String zzz = actual.getString("zzz");
                if (!order.isEmpty()) {
                    assertTrue(zzz.compareTo(prev) > 0);
                    prev = zzz;
                }
                var l = ZZZ_MAP.get(zzz);
                assertEquals(l.stream().mapToInt(TestEntity::getFoo).min().getAsInt(), actual.getInt("foo"));
                assertEquals(l.stream().mapToLong(TestEntity::getBar).sum(), actual.getLong("bar"));
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
    void selectGroupBySameKey(String groupKey) throws Exception {
        var sql = "select foo, count(*) as cnt from " + TEST + " group by " + groupKey;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(LIST.size(), list.size());

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
            assertContains("compile failed with error:invalid_aggregation_column message:\"column must be aggregated\" location:<input>:", e.getMessage()); // TODO エラー情報詳細
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
            assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
            assertContains("compile failed with error:symbol_not_found message:\"symbol 'k' is not found\" location:<input>:", e.getMessage());
        }
    }

    @Test
    void selectKeyOrderByCount() throws Exception {
        var sql = "select zzz, count(*) as cnt from " + TEST + " group by zzz order by count(*) desc";

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(ZZZ_MAP.size(), list.size());

        int prev = Integer.MAX_VALUE;
        for (var actual : list) {
            int count = actual.getInt("cnt");
            assertTrue(count <= prev);
            prev = count;

            String zzz = actual.getString("zzz");
            assertEquals(ZZZ_MAP.get(zzz).size(), actual.getInt("cnt"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", " order by zzz" })
    void having(String order) throws Exception {
        var sql = "select zzz, count(*) cnt from " + TEST + " group by zzz having count(*) >= 2" + order;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);

        var expectedMap = new HashMap<String, List<TestEntity>>();
        ZZZ_MAP.forEach((k, l) -> {
            if (l.size() >= 2) {
                expectedMap.put(k, l);
            }
        });
        assertEquals(expectedMap.size(), list.size());

        String prev = "";
        for (var actual : list) {
            String zzz = actual.getString("zzz");
            if (!order.isEmpty()) {
                assertTrue(zzz.compareTo(prev) > 0);
                prev = zzz;
            }
            assertEquals(expectedMap.get(zzz).size(), actual.getInt("cnt"));
        }
    }
}
