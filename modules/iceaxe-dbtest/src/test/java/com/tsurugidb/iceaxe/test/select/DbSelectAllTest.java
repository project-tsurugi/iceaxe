package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select all test
 */
class DbSelectAllTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void selectWhileInsertOcc() throws Exception {
        selectWhileInsert(TgTxOption.ofOCC());
    }

    @Test
    void selectWhileInserLtx() throws Exception {
        selectWhileInsert(TgTxOption.ofLTX());
    }

    @Test
    void selectWhileInserRtx() throws Exception {
        selectWhileInsert(TgTxOption.ofRTX());
    }

    private static final int COMMIT_SIZE = 100;

    private void selectWhileInsert(TgTxOption txOption) throws Exception {
        var session = getSession();
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            var insertThread = new InsertThread();
            insertThread.start();

            Throwable occurred = null;
            try {
                while (!insertThread.end) {
                    try (var transaction = session.createTransaction(txOption)) {
                        var list = transaction.executeAndGetList(ps);
                        int count = list.size();

                        String message = null;
                        if (count % COMMIT_SIZE != 0) {
                            message = MessageFormat.format("count={0} (COMMIT_SIZE={1})", count, COMMIT_SIZE);
//                          fail(message);
                            LOG.info(message);
                        }

                        if (message == null) {
                            transaction.commit(TgCommitType.DEFAULT);
                        } else {
                            try {
                                var e = assertThrows(TsurugiTransactionException.class, () -> {
                                    transaction.commit(TgCommitType.DEFAULT);
                                });
                                assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                                continue;
                            } catch (Throwable t) {
                                t.addSuppressed(new AssertionError(message));
                                throw t;
                            }
                        }
                    } catch (TsurugiTransactionException e) {
                        var exceptionUtil = TsurugiExceptionUtil.getInstance();
                        if (exceptionUtil.isSerializationFailure(e)) {
                            continue;
                        }
                        throw e;
                    }
                }
            } catch (Throwable e) {
                occurred = e;
                throw e;
            } finally {
                insertThread.join();

                var e = insertThread.exception;
                if (e != null) {
                    if (occurred != null) {
                        occurred.addSuppressed(e);
                    } else {
                        if (e instanceof Exception) {
                            throw (Exception) e;
                        }
                        throw new Exception(e);
                    }
                }
            }
        }
    }

    private static class InsertThread extends Thread {
        private volatile boolean end = false;
        private Throwable exception;

        @Override
        public void run() {
            try {
                execute();
            } catch (Throwable e) {
                this.exception = e;
            } finally {
                this.end = true;
            }
        }

        private void execute() throws Exception {
            try (var session = DbTestConnector.createSession()) {
                var tm = session.createTransactionManager(TgTxOption.ofOCC());
                try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                    var foo = new AtomicInteger(0);
                    for (int i = 0; i < 20; i++) {
                        tm.execute((TsurugiTransactionAction) transaction -> {
                            for (int j = 0; j < COMMIT_SIZE; j++) {
                                var entity = createTestEntity(foo.getAndIncrement());
                                transaction.executeAndGetCount(ps, entity);
                            }
                        });
                    }
                }
            }
        }
    }
}
