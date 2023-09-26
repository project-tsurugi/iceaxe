package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * multi thread insert test
 */
class DbInsertMultiThread2Test extends DbTestTableTester {
    private static final String TEST2 = "test2";

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(4);

        createTest2Table();

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
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
    void insertMultiTxOcc1(boolean prepare) throws Exception {
//      insertMultiTx(100, 1, TgTmSetting.of(TgTxOption.ofOCC()), prepare);
        insertMultiTxOcc(1, prepare);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    @Disabled // TODO remove Disabled. CC_OCC_PHANTOM_AVOIDANCE retry-over
    void insertMultiTxOcc30(boolean prepare) throws Exception {
        insertMultiTxOcc(30, prepare);
    }

    @RepeatedTest(4)
    @Disabled // TODO remove Disabled. CC_OCC_PHANTOM_AVOIDANCE retry-over
    void insertMultiTxOcc30False() throws Exception {
        insertMultiTxOcc(30, false);
    }

    private void insertMultiTxOcc(int threadSize, boolean prepare) throws IOException, InterruptedException {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC(), 20); // TODO リトライ無しにしたい
        setting.getTransactionOptionSupplier().setTmOptionListener((attempt, exception, tmOption) -> {
            if (attempt > 0) {
                LOG.info("insertMultiTxOcc({}, {}) OCC retry {}", threadSize, prepare, attempt);
            }
        });
        insertMultiTx(100, threadSize, setting, prepare);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void insertMultiTxLtx(boolean prepare) throws Exception {
        insertMultiTx(100, 30, TgTmSetting.of(TgTxOption.ofLTX(TEST2)), prepare);
    }

    private void insertMultiTx(int recordSize, int threadSize, TgTmSetting setting, boolean prepare) throws IOException, InterruptedException {
        var key1 = TgBindVariable.ofInt("key1");
        var deleteSql = "delete from " + TEST2 + " where key1=" + key1;
        var deleteMapping = TgParameterMapping.of(key1);
        var insertSql = "insert into " + TEST2 + "(key1,key2,key3,value1) values(:key1,:key2,:key3,:value1)";
        var insertMapping = TgParameterMapping.of(Test2Entity.class) //
                .addInt("key1", Test2Entity::getKey1) //
                .addLong("key2", Test2Entity::getKey2) //
                .addDate("key3", Test2Entity::getKey3) //
                .addString("value1", Test2Entity::getValue1);

        var excptionList = new ArrayList<Exception>();
        var session = getSession();
        try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                var deletePs = session.createStatement(deleteSql, deleteMapping); //
                var insertPs = session.createStatement(insertSql, insertMapping)) {
            if (prepare) {
                var tm = session.createTransactionManager(TgTmSetting.of(TgTxOption.ofLTX(TEST2)));
                tm.execute(transaction -> {
                    for (int i = 0; i < threadSize; i++) {
                        for (int j = 0; j < 2; j++) {
                            var entity = new Test2Entity(i, 1 + j);
                            transaction.executeAndGetCount(insertPs, entity);
                        }
                    }
                    return;
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

        if (!excptionList.isEmpty()) {
            String message = excptionList.stream().map(e -> e.getMessage()).collect(Collectors.joining("\n"));
            var re = new RuntimeException(message);
            for (var e : excptionList) {
                re.addSuppressed(e);
            }
            throw re;
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
        private final TsurugiSqlQuery<TestEntity> selectPs;
        private final TsurugiSqlPreparedStatement<TgBindParameters> deletePs;
        private final TsurugiSqlPreparedStatement<Test2Entity> insertPs;
        private final int number;
        private final int recordSize;
        private final TsurugiTransactionManager tm;
        private Exception exception;

        public InsertMultiTxThread(TsurugiSqlQuery<TestEntity> selectPs, TsurugiSqlPreparedStatement<TgBindParameters> deletePs, TsurugiSqlPreparedStatement<Test2Entity> insertPs, int number,
                int recordSize, TsurugiTransactionManager tm) {
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
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void runInTransaction(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
            var parameter = TgBindParameters.of(TgBindParameter.of("key1", number));
            transaction.executeAndGetCount(deletePs, parameter);

            for (int i = 0; i < recordSize; i++) {
                transaction.executeAndGetList(selectPs);

                var entity = new Test2Entity(number, i);
                transaction.executeAndGetCount(insertPs, entity);
            }
        }

        public Exception getException() {
            return this.exception;
        }
    }
}
