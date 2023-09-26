package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.fail;

import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

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
    @Disabled // TODO remove Disabled. countがinsert/commit件数と一致しないことがある
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
        var tm = session.createTransactionManager(TgTmSetting.ofAlways(txOption));
        try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
            var insertThread = new InsertThread();
            insertThread.start();

            Throwable occurred = null;
            try {
                while (!insertThread.end) {
                    tm.execute(transaction -> {
                        var list = transaction.executeAndGetList(ps);
                        int count = list.size();
                        if (count % COMMIT_SIZE != 0) {
                            fail(MessageFormat.format("count={0} (COMMIT_SIZE={1})", count, COMMIT_SIZE));
                        }
                    });
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
