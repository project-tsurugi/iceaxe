package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * irregular insert test
 */
class DbInsertIrregularTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void closePsBeforeCloseRc() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var ps = session.createPreparedStatement(sql);
            var rc = ps.execute(transaction);
            ps.close();
            int count = rc.getUpdateCount();
            assertEquals(-1, count); // TODO 1
            rc.close();
        });
    }
}
