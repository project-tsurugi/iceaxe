package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select alias test
 */
class DbSelectAliasTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectAliasTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void selectAsName() throws IOException {
        var sql = "select count(*) as count from " + TEST;
        selectCount(sql, "count");
    }

    @Test
    void selectName() throws IOException {
        var sql = "select count(*) cnt from " + TEST;
        selectCount(sql, "cnt");
    }

    @Test
    void selectNoName() throws IOException {
        var sql = "select count(*) from " + TEST;
        selectCount(sql, "@#0");
    }

    private void selectCount(String sql, String name) throws IOException {
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of(name), nameList);
            return record.getInt4(name);
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            int count = ps.executeAndFindRecord(tm).get();
            assertEquals(SIZE, count);
        }
    }

    @Test
    void selectNoName2() throws IOException {
        var sql = "select count(*), count(foo) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of("@#0", "@#1"), nameList);
            return TsurugiResultEntity.of(record);
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            tm.execute(transaction -> {
                try (var rs = ps.execute(transaction)) {
                    List<String> nameList = rs.getNameList();
                    assertEquals(List.of("@#0", "@#1"), nameList);
                    TsurugiResultEntity entity = rs.findRecord().get();
                    assertEquals(SIZE, entity.getInt4("@#0"));
                    assertEquals(SIZE, entity.getInt4("@#1"));
                }
            });
        }
    }
}
