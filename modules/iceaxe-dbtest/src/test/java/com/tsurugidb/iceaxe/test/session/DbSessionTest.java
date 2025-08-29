package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * session test
 */
class DbSessionTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSessionTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void doNothing() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            // do nothing
        }
    }

    @Test
    void getUserName() throws Exception {
        String user = DbTestConnector.getUser();
        assumeFalse(user == null, "user not specified");
        String password = DbTestConnector.getPassword();
        var credential = new UsernamePasswordCredential(user, password);

        try (var session = DbTestConnector.createSession(credential, "getUserName test")) {
            Optional<String> actual = session.getUserName();
            assertEquals(Optional.of(user), actual);
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

    @ParameterizedTest
    @ValueSource(strings = { "NOTHING", "GRACEFUL", "FORCEFUL" })
    void closeTwice(String s) throws Exception {
        var shutdownType = TgSessionShutdownType.valueOf(s);
        try (var session = DbTestConnector.createSession(shutdownType)) {
            session.getLowSqlClient();
            assertTrue(session.isAlive());

            session.close();
            assertFalse(session.isAlive());
        }
    }
}
