package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select alias test
 */
class DbSelectAliasTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectAliasTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @ParameterizedTest
    @ValueSource(strings = { " as", "" })
    void selectTableAlias(String as) throws Exception {
        var sql = "select t.foo, t.bar, t.zzz from " + TEST + as + " t order by t.foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(SIZE, list.size());
            for (int i = 0; i < SIZE; i++) {
                var expected = createTestEntity(i);
                var actual = list.get(i);
                assertEquals(expected, actual);
            }
        }
    }

    @Test
    void selectAsName() throws Exception {
        var sql = "select count(*) as count from " + TEST;
        selectCount(sql, "count");
    }

    @Test
    void selectName() throws Exception {
        var sql = "select count(*) cnt from " + TEST;
        selectCount(sql, "cnt");
    }

    @Test
    void selectNoName() throws Exception {
        var sql = "select count(*) from " + TEST;
        selectCount(sql, "@#0");
    }

    private void selectCount(String sql, String name) throws Exception {
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of(name), nameList);
            return record.getInt(name);
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(SIZE, count);
        }
    }

    @Test
    void selectNoName2() throws Exception {
        var sql = "select count(*), count(foo) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of("@#0", "@#1"), nameList);
            return TsurugiResultEntity.of(record);
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    List<String> nameList = result.getNameList();
                    assertEquals(List.of("@#0", "@#1"), nameList);
                    TsurugiResultEntity entity = result.findRecord().get();
                    assertEquals(SIZE, entity.getInt("@#0"));
                    assertEquals(SIZE, entity.getInt("@#1"));
                }
            });
        }
    }
}
