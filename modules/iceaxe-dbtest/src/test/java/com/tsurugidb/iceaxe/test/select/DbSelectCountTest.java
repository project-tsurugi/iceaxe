package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * select count test
 */
class DbSelectCountTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_converter(int size) throws Exception {
        count(size, TgResultMapping.of(record -> record.nextInt()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_singleColumn_class(int size) throws Exception {
        count(size, TgResultMapping.ofSingle(int.class));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_singleColumn_type(int size) throws Exception {
        count(size, TgResultMapping.ofSingle(TgDataType.INT));
    }

    private void count(int size, TgResultMapping<Integer> resultMapping) throws IOException, InterruptedException {
        insertTestTable(size);

        var sql = "select count(*) from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(size, count);
        }
    }

    @Test
    @Disabled // TODO remove Disabled. countがinsert/commit件数と一致しないことがある
    void countWhileInsertOcc() throws Exception {
        countWhileInsert(TgTxOption.ofOCC());
    }

    @Test
    void countWhileInserLtx() throws Exception {
        countWhileInsert(TgTxOption.ofLTX());
    }

    @Test
    void countWhileInserRtx() throws Exception {
        countWhileInsert(TgTxOption.ofRTX());
    }

    private static final int COMMIT_SIZE = 100;

    private void countWhileInsert(TgTxOption txOption) throws Exception {
        var sql = "select count(*) from " + TEST;

        var session = getSession();
        var tm = session.createTransactionManager(TgTmSetting.ofAlways(txOption));
        try (var ps = session.createQuery(sql, TgParameterMapping.of(), TgResultMapping.ofSingle(int.class))) {
            var insertThread = new InsertThread();
            insertThread.start();

            Throwable occurred = null;
            try {
                while (!insertThread.end) {
                    tm.execute(transaction -> {
                        int count = transaction.executeAndFindRecord(ps, null).orElse(0);
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
