package com.tsurugidb.iceaxe.test.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * irregular delete test
 */
class DbDeleteIrregularTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void closePsBeforeCloseRc() throws IOException {
        int number = SIZE / 2;
        var sql = "delete from " + TEST //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var ps = session.createPreparedStatement(sql);
            var rc = ps.execute(transaction);
            ps.close();
            int count = rc.getUpdateCount();
            assertUpdateCount(1, count);
            rc.close();
        });

        var list = selectAllFromTest();
        assertEquals(SIZE - 1, list.size());
        for (var entity : list) {
            assertNotEquals(number, entity.getFoo());

            int i = entity.getFoo();
            var expected = createTestEntity(i);
            assertEquals(expected, entity);
        }
    }
}
