package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * multiple session test
 */
class DbMultiSessionTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = 260;
    private static final int EXPECTED_SESSION_SIZE = getSystemProperty("expected.session.size", 100);

    @Test
    void limit() throws IOException {
        var list = new ArrayList<TsurugiSession>();
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            var session = DbTestConnector.createSession();
            list.add(session);
        }

        try {
            limit(list);
        } finally {
            for (var session : list) {
                session.close();
            }
        }
    }

    private void limit(List<TsurugiSession> list) {
        int count = 0;
        for (var session : list) {
            if (session.isAlive()) {
                count++;
            } else {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> {
                    session.getLowSqlClient();
                });
                assertEqualsCode(IceaxeErrorCode.SESSION_LOW_ERROR, e);
                var c = e.getCause();
                assertEquals("the server has declined the connection request", c.getMessage());
            }
        }
        if (count < EXPECTED_SESSION_SIZE) {
            fail(MessageFormat.format("less session.size expected: {1} but was: {0}", count, EXPECTED_SESSION_SIZE));
        }
    }

    @Test
    void manySession1() throws IOException {
        manySession(false, false);
    }

    @Test
    void manySession2() throws IOException {
        manySession(true, false);
    }

    @Test
    void manySession3() throws IOException {
        manySession(false, true);
    }

    private void manySession(boolean sqlClient, boolean transaction) throws IOException {
        LOG.debug("create session start");
        var list = new ArrayList<TsurugiSession>();
        for (int i = 0; i < 60; i++) {
            var session = DbTestConnector.createSession();
            list.add(session);

            if (sqlClient) {
                session.getLowSqlClient();
            }
        }
        LOG.debug("create session end");

        if (transaction) {
            LOG.debug("createTransaction start");
            int i = 0;
            for (var session : list) {
                LOG.debug("createTransaction {}", i++);
                try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                    tx.getLowTransaction();
                }
            }
            LOG.debug("createTransaction end");
        }

        LOG.debug("close session start");
        for (var session : list) {
            session.close();
        }
        LOG.debug("close session end");
    }

    @Test
    void multiThread() {
        LOG.debug("create session start");
        var sessionList = new CopyOnWriteArrayList<TsurugiSession>();
        var threadList = new ArrayList<Thread>();
        var alive = new AtomicBoolean(true);
        for (int i = 0; i < 60; i++) {
            var thread = new Thread(() -> {
                TsurugiSession session;
                try {
                    session = DbTestConnector.createSession();
                    sessionList.add(session);
                    session.getLowSqlClient();
                } catch (Exception e) {
                    LOG.warn("connect error. {}: {}", e.getClass().getName(), e.getMessage());
                    return;
                }

                while (alive.get()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            threadList.add(thread);
            thread.start();
        }
        LOG.debug("create session end");

        alive.set(false);

        LOG.debug("thread join start");
        for (var thread : threadList) {
            try {
                thread.join();
            } catch (Exception e) {
                LOG.warn("join error", e);
            }
        }
        LOG.debug("thread join end");

        LOG.debug("close session start");
        for (var session : sessionList) {
            try {
                session.close();
            } catch (Exception e) {
                LOG.warn("close error", e);
            }
        }
        LOG.debug("close session end");
    }
}
