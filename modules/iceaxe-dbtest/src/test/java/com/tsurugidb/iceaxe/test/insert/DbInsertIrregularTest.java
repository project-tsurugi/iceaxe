package com.tsurugidb.iceaxe.test.insert;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * irregular insert test
 */
class DbInsertIrregularTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void closePsBeforeCloseRc() throws Exception {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(123, 456, 'abc')";

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
    }
}
