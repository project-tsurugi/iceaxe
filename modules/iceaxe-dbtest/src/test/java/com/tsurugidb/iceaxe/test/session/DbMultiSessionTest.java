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

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * multiple session test
 */
class DbMultiSessionTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = 260;

    // TODO 最大接続数は、実行環境（tsurugidbの構成定義ファイルの内容）によって変わるので、本当はDBサーバーから取得したい
    private static final int EXPECTED_SESSION_SIZE = getSystemProperty("tsurugi.dbtest.expected.session.size", 104);

    @RepeatedTest(10)
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbMultiSessionTest-limit.*")
    void limit() throws Exception {
        closeStaticSession();

        var sessionList = new ArrayList<TsurugiSession>();
        Throwable occurred = null;
        try {
            String baseLabel = DbTestConnector.getSessionLabel();
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                TsurugiSession session;
                try {
                    session = DbTestConnector.createSession(baseLabel + "[" + i + "]");
                } catch (IOException e) {
                    assertEqualsMessage("the server has declined the connection request", e);
                    int count = sessionList.size();
                    if (count < EXPECTED_SESSION_SIZE) {
                        fail(MessageFormat.format("less session.size expected: {1} but was: {0}", count, EXPECTED_SESSION_SIZE));
                    }
                    return;
                }
                sessionList.add(session);
            }

            limit(sessionList);
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            closeSession(sessionList, occurred);
        }
    }

    private void limit(List<TsurugiSession> list) {
        int count = 0;
        for (var session : list) {
            if (session.isAlive()) {
                count++;
            } else {
                var e = assertThrowsExactly(IceaxeIOException.class, () -> {
                    session.getLowSqlClient();
                });
                assertEqualsCode(IceaxeErrorCode.SESSION_LOW_ERROR, e);
                var c = e.getCause();
                try {
                    assertEquals("the server has declined the connection request", c.getMessage());
                } catch (Throwable t) {
                    t.addSuppressed(e);
                    throw t;
                }
            }
        }
        if (count < EXPECTED_SESSION_SIZE) {
            fail(MessageFormat.format("less session.size expected: {1} but was: {0}", count, EXPECTED_SESSION_SIZE));
        }
    }

    @Test
    void manySession1() throws Exception {
        manySession(false, false);
    }

    @Test
    void manySession2() throws Exception {
        manySession(true, false);
    }

    @Test
    void manySession3() throws Exception {
        manySession(false, true);
    }

    private void manySession(boolean sqlClient, boolean transaction) throws IOException, InterruptedException {
        LOG.debug("create session start");
        var sessionList = new ArrayList<TsurugiSession>();
        Throwable occurred = null;
        try {
            String baseLabel = DbTestConnector.getSessionLabel();
            for (int i = 0; i < 60; i++) {
                var session = DbTestConnector.createSession(baseLabel + "[" + i + "]");
                sessionList.add(session);

                if (sqlClient) {
                    session.getLowSqlClient();
                }
            }
            LOG.debug("create session end");

            if (transaction) {
                LOG.debug("createTransaction start");
                int i = 0;
                for (var session : sessionList) {
                    LOG.debug("createTransaction {}", i++);
                    try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                        tx.getLowTransaction();
                    }
                }
                LOG.debug("createTransaction end");
            }
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            closeSession(sessionList, occurred);
        }
    }

    @Test
    void multiThread() {
        LOG.debug("create session start");
        var sessionList = new CopyOnWriteArrayList<TsurugiSession>();
        Throwable occurred = null;
        try {
            var threadList = new ArrayList<Thread>();
            var alive = new AtomicBoolean(true);
            String baseLabel = DbTestConnector.getSessionLabel();
            for (int i = 0; i < 60; i++) {
                String label = baseLabel + "[" + i + "]";
                var thread = new Thread(() -> {
                    TsurugiSession session;
                    try {
                        session = DbTestConnector.createSession(label);
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
        } catch (Throwable e) {
            occurred = e;
            throw e;
        } finally {
            closeSession(sessionList, occurred);
        }
    }

    private void closeSession(List<TsurugiSession> sessionList, Throwable occurred) {
        LOG.debug("close session start");

        var exceptionList = new ArrayList<Exception>();
        for (var session : sessionList) {
            try {
                session.close();
            } catch (Exception e) {
                exceptionList.add(e);
            }
        }

        if (!exceptionList.isEmpty()) {
            var e = new RuntimeException("session close error. errorCount=" + exceptionList.size());
            exceptionList.forEach(e::addSuppressed);
            if (occurred != null) {
                occurred.addSuppressed(e);
            } else {
                throw e;
            }
        }

        LOG.debug("close session end");
    }
}
