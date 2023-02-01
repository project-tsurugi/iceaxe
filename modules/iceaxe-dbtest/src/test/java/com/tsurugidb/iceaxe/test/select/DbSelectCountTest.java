package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select count test
 */
class DbSelectCountTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectCountTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();

        LOG.debug("init end");
    }

    @Test
    void count0() throws IOException {
        count(0);
    }

    @Test
    void countN() throws IOException {
        count(3);
    }

    private void count(int size) throws IOException {
        insertTestTable(size);

        var sql = "select count(*) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> record.nextInt4());

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(size, count);
        }
    }
}
