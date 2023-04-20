package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * session test
 */
class DbSessionTest extends DbTestTableTester {

    @Test
    void doNothing() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            // do nothing
        }
    }

    @Test
    void isAlive() throws Exception {
        // session作成直後にisAlive呼び出し
        try (var session = DbTestConnector.createSession()) {
            assertTrue(session.isAlive());
        }
    }

    @Test
    void preparedStatementOnly() throws Exception {
        var sql = "select * from " + TEST;
        try (var session = DbTestConnector.createSession()) {
            try (var ps = session.createQuery(sql, TgParameterMapping.of())) {
                assertTrue(session.isAlive());
            }
        }
    }

    @Test
    void transactionOnly() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                assertTrue(session.isAlive());
            }
        }
    }

    @Test
    void closeTwice() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            session.getLowSqlClient();
            assertTrue(session.isAlive());

            session.close();
            assertFalse(session.isAlive());
        }
    }
}
