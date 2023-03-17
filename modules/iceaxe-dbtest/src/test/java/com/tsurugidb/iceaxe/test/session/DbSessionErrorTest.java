package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * session error test
 */
class DbSessionErrorTest extends DbTestTableTester {

    @Test
    void createQueryAfterClose() throws IOException {
        var session = createClosedSession();

        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            session.createQuery(SELECT_SQL);
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createPreparedQueryAfterClose() throws IOException {
        var session = createClosedSession();

        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            session.createQuery(SELECT_SQL, TgParameterMapping.of());
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createStatementAfterClose() throws IOException {
        var session = createClosedSession();

        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            session.createStatement(INSERT_SQL);
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createPreparedStatementAfterClose() throws IOException {
        var session = createClosedSession();

        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            session.createStatement(INSERT_SQL, INSERT_MAPPING);
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void createTransactionAfterClose() throws IOException {
        var session = createClosedSession();

        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            session.createTransaction(TgTxOption.ofOCC());
        });
        assertEqualsCode(IceaxeErrorCode.SESSION_ALREADY_CLOSED, e);
    }

    @Test
    void craeteTmAfterClose() throws IOException {
        var session = createClosedSession();

        session.createTransactionManager(); // not thrown
    }

    private TsurugiSession createClosedSession() throws IOException {
        var session = DbTestConnector.createSession();
        try {
            assertTrue(session.isAlive());
        } catch (Throwable t) {
            session.close();
            throw t;
        }
        session.close();
        assertFalse(session.isAlive());

        return session;
    }
}
