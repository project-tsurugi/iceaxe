package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

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

    @Test
    void insertMultiTxOcc() throws IOException, InterruptedException {
        insertMultiTx(100, 30, TgTxOption.ofOCC());
    }

    @ParameterizedTest
    @ValueSource(ints = { 8 /* 2, 3, 8, 30 */ })
    @Disabled // TODO remove Disabled
    void insertMultiTxLtx(int threadSize) throws IOException, InterruptedException {
        insertMultiTx(100, threadSize, TgTxOption.ofLTX(TEST2));
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

    private void insertMultiTx(int recordSize, int threadSize, TgTxOption tx) throws IOException {
        var key1 = TgVariable.ofInt4("key1");
        var deleteSql = "delete from " + TEST2 + " where key1=" + key1;
        var deleteMapping = TgParameterMapping.of(key1);
        var insertSql = "insert into " + TEST2 + "(key1,key2,key3,value1) values(:key1,:key2,:key3,:value1)";
        var insertMapping = TgParameterMapping.of(Test2Entity.class) //
                .int4("key1", Test2Entity::getKey1) //
                .int8("key2", Test2Entity::getKey2) //
                .date("key3", Test2Entity::getKey3) //
                .character("value1", Test2Entity::getValue1);

        var session = getSession();
        var tm = session.createTransactionManager(tx);
        try (var selectPs = session.createPreparedQuery(SELECT_SQL, SELECT_MAPPING); //
                var deletePs = session.createPreparedStatement(deleteSql, deleteMapping); //
                var insertPs = session.createPreparedStatement(insertSql, insertMapping)) {
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
            }
        }

        var actual = selectCountFrom(TEST2);
        assertEquals(recordSize * threadSize, actual);
    }

    private static class InsertMultiTxThread extends Thread {
        private final TsurugiPreparedStatementQuery0<TestEntity> selectPs;
        private final TsurugiPreparedStatementUpdate1<TgParameterList> deletePs;
        private final TsurugiPreparedStatementUpdate1<Test2Entity> insertPs;
        private final int number;
        private final int recordSize;
        private final TsurugiTransactionManager tm;

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
                throw new UncheckedIOException(e);
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
    }
}
