package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;

/**
 * select empty-table test
 */
class DbSelectEmptyTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectEmptyTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();

        LOG.debug("init end");
    }

    @Test
    void selectEmpty() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            for (int i = 0; i < 300; i++) {
                List<TsurugiResultEntity> list = ps.executeAndGetList(tm);
                assertEquals(List.of(), list);
            }
        }
    }

    @Test
    void selectEmptySameTx() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            tm.execute((TsurugiTransactionAction) transaction -> {
                for (int i = 0; i < 300; i++) {
                    List<TsurugiResultEntity> list = ps.executeAndGetList(transaction);
                    assertEquals(List.of(), list);
                }
            });
        }
    }

    @Test
    void selectCount() throws IOException {
        var sql = "select count(*) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            int count = ps.executeAndFindRecord(tm).get();
            assertEquals(0, count);
        }
    }

    @Test
    void selectSum() throws IOException {
        var sql = "select sum(bar) as sum, min(zzz) as zzz from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            TsurugiResultEntity entity = ps.executeAndFindRecord(tm).get();
            assertNull(entity.getInt4OrNull("sum"));
            assertNull(entity.getCharacterOrNull("zzz"));
        }
    }
}
