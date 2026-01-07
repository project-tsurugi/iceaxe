package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * select distinct test
 */
class DbSelectDistinctTest extends DbTestTableTester {

    private static final List<TestEntity> LIST = IntStream.range(0, 8).mapToObj(i -> new TestEntity(i, i % 4, Integer.toString(i % 4))).collect(Collectors.toList());

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectDistinctTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        for (var entity : LIST) {
            insertTestTable(entity);
        }

        logInitEnd(LOG, info);
    }

    @Test
    void selectDistinctAll() throws Exception {
        String sql = "select distinct * from " + TEST + " order by foo";

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql, SELECT_MAPPING);
        assertEquals(LIST, list);
    }

    @Test
    void selectDistinctBarZzz() throws Exception {
        String sql = "select distinct bar, zzz from " + TEST + " order by bar";

        var tm = createTransactionManagerOcc(getSession());
        var resultMapping = TgResultMapping.of(TestEntity::new).addLong("bar", TestEntity::setBar).addString("zzz", TestEntity::setZzz);
        var list = tm.executeAndGetList(sql, resultMapping);

        var expectedList = LIST.stream().peek(entity -> entity.setFoo(0)).distinct().sorted(Comparator.comparing(TestEntity::getBar)).collect(Collectors.toList());
        assertEquals(expectedList.size(), list.size());
        for (int i = 0; i < expectedList.size(); i++) {
            var expected = expectedList.get(i);
            var actual = list.get(i);
            assertEquals(expected.getBar(), actual.getBar());
        }
    }
}
