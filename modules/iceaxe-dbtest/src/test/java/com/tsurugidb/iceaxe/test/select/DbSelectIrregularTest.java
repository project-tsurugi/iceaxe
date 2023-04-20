package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * irregular select test
 */
class DbSelectIrregularTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectIrregularTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void closePsBeforeCloseRs() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var ps = session.createQuery(sql);
            var result = ps.execute(transaction);
            ps.close();
            var list = result.getRecordList();
            assertEquals(SIZE, list.size());
            result.close();
        });
    }
}
