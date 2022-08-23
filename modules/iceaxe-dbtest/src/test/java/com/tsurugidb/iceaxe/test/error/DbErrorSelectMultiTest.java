package com.tsurugidb.iceaxe.test.error;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * multi select error test
 */
class DbErrorSelectMultiTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll() throws IOException {
        dropTestTable();
        createTestTable();
        insertTestTable(1);
    }

    @Test
    @Disabled // TODO remove Disabled
    void selectMulti1() throws IOException, TsurugiTransactionException {
        for (int i = 0; i < 100; i++) {
            selectMulti(1);
        }
    }

    @Test
    void selectMultiSuccess() throws IOException, TsurugiTransactionException {
        selectMulti(15);
    }

    @Test
    @Disabled // TODO change Disabled to Timeout
    void selectMultiFail() throws IOException, TsurugiTransactionException {
        selectMulti(16);
    }

    private void selectMulti(int size) throws IOException, TsurugiTransactionException {
        var sql = "select * from " + TEST;

        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < size; i++) {
                var transaction = session.createTransaction(TgTxOption.ofOCC());
                // transaction.close is called on session.close

                var ps = session.createPreparedQuery(sql);
                // ps.close is called on session.close

                @SuppressWarnings("unused")
                var rs = ps.execute(transaction);
                // rs.close is called on ps.close

                // do not commit/rollback
            }
        }
    }
}
