package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * irregular update test
 */
class DbUpdateIrregularTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void closePsBeforeCloseRc() throws Exception {
        int number = SIZE / 2;
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var ps = session.createStatement(sql);
            var result = ps.execute(transaction);
            ps.close();
            int count = result.getUpdateCount();
            assertUpdateCount(1, count);
            result.close();
        });

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            if (entity.getFoo() == number) {
                assertEquals(0L, entity.getBar());
                assertEquals("aaa", entity.getZzz());
            } else {
                int i = entity.getFoo();
                var expected = createTestEntity(i);
                assertEquals(expected, entity);
            }
        }
    }
}
