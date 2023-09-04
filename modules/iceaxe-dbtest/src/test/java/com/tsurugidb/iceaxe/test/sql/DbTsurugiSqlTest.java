package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.FutureResponseCloseWrapper;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;

/**
 * {@link TsurugiSql} test
 */
class DbTsurugiSqlTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void queryConstructorError() throws Exception {
        var session = DbTestConnector.createSession();

        session.close();
        var e = assertThrows(TsurugiIOException.class, () -> new TsurugiSqlQuery<>(session, SELECT_SQL, SELECT_MAPPING));
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void statementConstructorError() throws Exception {
        var session = DbTestConnector.createSession();

        session.close();
        var e = assertThrows(TsurugiIOException.class, () -> new TsurugiSqlStatement(session, INSERT_SQL));
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void preparedQueryConstructorError() throws Exception {
        var session = DbTestConnector.createSession();
        FutureResponseCloseWrapper<PreparedStatement> future;
        try (var client = SqlClient.attach(session.getLowSession())) {
            future = FutureResponseCloseWrapper.of(client.prepare(SELECT_SQL));
        }

        session.close();
        var e = assertThrows(TsurugiIOException.class, () -> new TsurugiSqlPreparedQuery<>(session, SELECT_SQL, future, null, SELECT_MAPPING));
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
        assertTrue(future.isClosed());
    }

    @Test
    void preparedStatementConstructorError() throws Exception {
        var session = DbTestConnector.createSession();
        FutureResponseCloseWrapper<PreparedStatement> future;
        try (var client = SqlClient.attach(session.getLowSession())) {
            future = FutureResponseCloseWrapper.of(client.prepare(INSERT_SQL, INSERT_MAPPING.toLowPlaceholderList()));
        }

        session.close();
        var e = assertThrows(TsurugiIOException.class, () -> new TsurugiSqlPreparedStatement<>(session, INSERT_SQL, future, INSERT_MAPPING));
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
        assertTrue(future.isClosed());
    }
}
