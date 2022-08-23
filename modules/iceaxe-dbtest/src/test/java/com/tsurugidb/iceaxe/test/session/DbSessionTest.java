package com.tsurugidb.iceaxe.test.session;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * Session test
 */
class DbSessionTest extends DbTestTableTester {

    @Test
    void doNothing() throws IOException {
        try (var session = DbTestConnector.createSession()) {
            // do nothing
        }
    }

    @Test
    void preparedStatementOnly() throws IOException {
        var sql = "select * from " + TEST;
        try (var session = DbTestConnector.createSession()) {
            try (var ps = session.createPreparedQuery(sql)) {
                // do nothing
            }
        }
    }

    @Test
    void transactionOnly() throws IOException {
        try (var session = DbTestConnector.createSession()) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                // do nothing
            }
        }
    }
}
