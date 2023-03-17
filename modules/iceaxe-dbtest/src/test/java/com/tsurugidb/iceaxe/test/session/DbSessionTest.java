package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

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
    void doNothing() throws IOException {
        try (var session = DbTestConnector.createSession()) {
            // do nothing
        }
    }

    @Test
    void isAlive() throws IOException {
        // session作成直後にisAlive呼び出し
        try (var session = DbTestConnector.createSession()) {
            assertTrue(session.isAlive());
        }
    }

    @Test
    void preparedStatementOnly() throws IOException {
        var sql = "select * from " + TEST;
        try (var session = DbTestConnector.createSession()) {
            try (var ps = session.createQuery(sql, TgParameterMapping.of())) {
                assertTrue(session.isAlive());
            }
        }
    }

    @Test
    void transactionOnly() throws IOException {
        try (var session = DbTestConnector.createSession()) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                assertTrue(session.isAlive());
            }
        }
    }

    @Test
    void closeTwice() throws IOException {
        try (var session = DbTestConnector.createSession()) {
            session.getLowSqlClient();
            assertTrue(session.isAlive());

            session.close();
            assertFalse(session.isAlive());
        }
    }
}
