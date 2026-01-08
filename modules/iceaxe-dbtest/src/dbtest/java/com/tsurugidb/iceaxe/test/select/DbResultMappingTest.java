package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * {@link TgResultMapping} test
 */
class DbResultMappingTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbResultMappingTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void fewColumn_sequential() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt(TestEntity::setFoo) //
                .addLong(TestEntity::setBar); //

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertFewColumn(actualList);
    }

    @Test
    void fewColumn_name() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt("foo", TestEntity::setFoo) //
                .addLong("bar", TestEntity::setBar); //

        var tm = createTransactionManagerOcc(getSession());
        var actualList = tm.executeAndGetList(sql, resultMapping);

        assertFewColumn(actualList);
    }

    private static void assertFewColumn(List<TestEntity> actualList) {
        assertEquals(SIZE, actualList.size());
        int i = 0;
        for (var actual : actualList) {
            assertEquals(i, actual.getFoo());
            assertEquals(i, actual.getBar());
            assertNull(actual.getZzz());
            i++;
        }
    }

    @Test
    void fewColumn_single() throws Exception {
        var sql = "select * from " + TEST + " order by foo";
        var resultMapping = TgResultMapping.ofSingle(int.class);

        var tm = createTransactionManagerOcc(getSession());
        List<Integer> actualList = tm.executeAndGetList(sql, resultMapping);

        assertEquals(SIZE, actualList.size());
        int i = 0;
        for (var actual : actualList) {
            assertEquals(i, actual);
            i++;
        }
    }
}
