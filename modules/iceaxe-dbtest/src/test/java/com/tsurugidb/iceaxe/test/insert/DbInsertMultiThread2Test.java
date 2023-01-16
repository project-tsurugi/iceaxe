package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * multi thread insert test
 */
class DbInsertMultiThread2Test extends DbTestTableTester {
    private static final String TEST2 = "test2";

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(4);

        createTest2Table();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTest2Table() throws IOException {
        dropTable(TEST2);
        var sql = "create table " + TEST2 //
                + "(" //
                + "  key1 int," //
                + "  key2 bigint," //
                + "  key3 date," //
                + "  value1 varchar(10)," //
                + "  primary key(key1, key2, key3)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static class Test2Entity {
        private int key1;
        private long key2;
        private LocalDate key3;
        private String value1;

        public Test2Entity(int key1, long key2) {
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = LocalDate.of(2022, 11, 2);
            this.value1 = Long.toString(key2);
        }

        public int getKey1() {
            return this.key1;
        }

        public long getKey2() {
            return this.key2;
        }

        public LocalDate getKey3() {
            return this.key3;
        }

        public String getValue1() {
            return this.value1;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void insertMultiTxOcc1(boolean prepare) throws IOException, InterruptedException {
//      insertMultiTx(100, 1, TgTmSetting.of(TgTxOption.ofOCC()), prepare);
        insertMultiTxOcc(1, prepare);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void insertMultiTxOcc30(boolean prepare) throws IOException, InterruptedException {
        insertMultiTxOcc(30, prepare);
    }

    @RepeatedTest(4)
    @Disabled // TODO remove Disabled たまにtateyama-serverでstd::bad_allocが発生する
    void insertMultiTxOcc30False() throws IOException, InterruptedException {
        insertMultiTxOcc(30, false);
    }

    private void insertMultiTxOcc(int threadSize, boolean prepare) throws IOException, InterruptedException {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC(), 20); // TODO リトライ無しにしたい
        setting.getTransactionOptionSupplier().setStateListener((attempt, e, state) -> {
            if (attempt > 0) {
                LOG.info("insertMultiTxOcc({}, {}) OCC retry {}", threadSize, prepare, attempt);
            }
        });
        insertMultiTx(100, threadSize, setting, prepare);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void insertMultiTxLtx(boolean prepare) throws IOException, InterruptedException {
        insertMultiTx(100, 30, TgTmSetting.of(TgTxOption.ofLTX(TEST2)), prepare);
    }

    private void insertMultiTx(int recordSize, int threadSize, TgTmSetting setting, boolean prepare) throws IOException {
        var key1 = TgVariable.ofInt4("key1");
        var deleteSql = "delete from " + TEST2 + " where key1=" + key1;
        var deleteMapping = TgParameterMapping.of(key1);
        var insertSql = "insert into " + TEST2 + "(key1,key2,key3,value1) values(:key1,:key2,:key3,:value1)";
        var insertMapping = TgParameterMapping.of(Test2Entity.class) //
                .int4("key1", Test2Entity::getKey1) //
                .int8("key2", Test2Entity::getKey2) //
                .date("key3", Test2Entity::getKey3) //
                .character("value1", Test2Entity::getValue1);

        var excptionList = new ArrayList<Exception>();
        var session = getSession();
        try (var selectPs = session.createPreparedQuery(SELECT_SQL, SELECT_MAPPING); //
                var deletePs = session.createPreparedStatement(deleteSql, deleteMapping); //
                var insertPs = session.createPreparedStatement(insertSql, insertMapping)) {
            if (prepare) {
                var tm = session.createTransactionManager(TgTmSetting.of(TgTxOption.ofLTX(TEST2)));
                tm.execute((TsurugiTransactionAction) transaction -> {
                    for (int i = 0; i < threadSize; i++) {
                        for (int j = 0; j < 2; j++) {
                            var entity = new Test2Entity(i, 1 + j);
                            insertPs.executeAndGetCount(transaction, entity);
                        }
                    }
                });
            }

            var tm = session.createTransactionManager(setting);
            var list = new ArrayList<InsertMultiTxThread>(threadSize);
            for (int i = 0; i < threadSize; i++) {
                var thread = new InsertMultiTxThread(selectPs, deletePs, insertPs, i, recordSize, tm);
                list.add(thread);
                thread.start();
            }
            for (var thread : list) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                var exception = thread.getException();
                if (exception != null) {
                    excptionList.add(exception);
                }
            }
        }

        var actual = selectCountFrom(TEST2);
        try {
            assertEquals(recordSize * threadSize, actual);
        } catch (Throwable t) {
            for (var e : excptionList) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    private static class InsertMultiTxThread extends Thread {
        private final TsurugiPreparedStatementQuery0<TestEntity> selectPs;
        private final TsurugiPreparedStatementUpdate1<TgParameterList> deletePs;
        private final TsurugiPreparedStatementUpdate1<Test2Entity> insertPs;
        private final int number;
        private final int recordSize;
        private final TsurugiTransactionManager tm;
        private Exception exception;

        public InsertMultiTxThread(TsurugiPreparedStatementQuery0<TestEntity> selectPs, TsurugiPreparedStatementUpdate1<TgParameterList> deletePs,
                TsurugiPreparedStatementUpdate1<Test2Entity> insertPs, int number, int recordSize, TsurugiTransactionManager tm) {
            this.selectPs = selectPs;
            this.deletePs = deletePs;
            this.insertPs = insertPs;
            this.number = number;
            this.recordSize = recordSize;
            this.tm = tm;
        }

        @Override
        public void run() {
            try {
                tm.execute(transaction -> {
                    runInTransaction(transaction);
                });
            } catch (IOException e) {
                this.exception = e;
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }

        private void runInTransaction(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
            var parameter = TgParameterList.of(TgParameter.of("key1", number));
            deletePs.executeAndGetCount(transaction, parameter);

            for (int i = 0; i < recordSize; i++) {
                selectPs.executeAndGetList(transaction);

                var entity = new Test2Entity(number, i);
                insertPs.executeAndGetCount(transaction, entity);
            }
        }

        public Exception getException() {
            return this.exception;
        }
    }
}
