package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * select empty-table test
 */
class DbSelectEmptyTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

    @BeforeAll
    static void beforeAll() throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectEmptyTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();

        LOG.debug("init end");
    }

    @Test
    void selectEmpty() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
                assertEquals(List.of(), list);
            }
        }
    }

    @Test
    void selectEmptySameTx() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            tm.execute(transaction -> {
                for (int i = 0; i < ATTEMPT_SIZE; i++) {
                    List<TsurugiResultEntity> list = transaction.executeAndGetList(ps);
                    assertEquals(List.of(), list);
                }
                return;
            });
        }
    }

    @Test
    void selectCount() throws Exception {
        selectCount("");
    }

    void selectCount(String where) throws IOException, InterruptedException {
        var sql = "select count(*) from " + TEST + where;
        var resultMapping = TgResultMapping.of(record -> record.nextInt());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(0, count);
        }
    }

    @Test
    void selectSum() throws Exception {
        selectSum("");
    }

    void selectSum(String where) throws IOException, InterruptedException {
        var sql = "select sum(bar) as sum, min(zzz) as zzz from " + TEST + where;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            TsurugiResultEntity entity = tm.executeAndFindRecord(ps).get();
            assertNull(entity.getIntOrNull("sum"));
            assertNull(entity.getStringOrNull("zzz"));
        }
    }

    @Test
    void selectKeyCount() throws Exception {
        selectKeyCount("");
    }

    void selectKeyCount(String where) throws IOException, InterruptedException {
        var sql = "select foo, count(*) from " + TEST //
                + where //
                + " group by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(0, list.size());
        }
    }

    @Test
    void selectKeySum() throws Exception {
        selectKeySum("");
    }

    void selectKeySum(String where) throws IOException, InterruptedException {
        var sql = "select foo, sum(bar) as sum, min(zzz) as zzz from " + TEST //
                + where //
                + " group by foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(0, list.size());
        }
    }
}
