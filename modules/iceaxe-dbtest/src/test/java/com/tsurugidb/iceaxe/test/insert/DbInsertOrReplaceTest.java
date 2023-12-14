package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * insert or replace test
 */
class DbInsertOrReplaceTest extends DbTestTableTester {

    private static final int SIZE = 10;
    private static final int INSERT_SIZE = 20;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void occ() throws Exception {
        test(TgTxOption.ofOCC());
    }

    @Test
    void ltx() throws Exception {
        test(TgTxOption.ofLTX(TEST));
    }

    private void test(TgTxOption txOption) throws Exception {
        test(txOption, new DbTestSessions());
    }

    private void test(TgTxOption txOption, DbTestSessions sessions) throws Exception {
        int threadSize = 10;

        try (sessions) {
            var onlineList = new ArrayList<OnlineTask>(threadSize);
            for (int i = 0; i < threadSize; i++) {
                var task = new OnlineTask(sessions.createSession(), txOption, i);
                onlineList.add(task);
            }

            var service = Executors.newCachedThreadPool();
            var futureList = new ArrayList<Future<?>>(threadSize);
            onlineList.forEach(task -> futureList.add(service.submit(task)));

            var exceptionList = new ArrayList<Exception>();
            for (var future : futureList) {
                try {
                    future.get();
                } catch (Exception e) {
                    exceptionList.add(e);
                }
            }
            if (!exceptionList.isEmpty()) {
                if (exceptionList.size() == 1) {
                    throw exceptionList.get(0);
                }
                var e = new Exception(exceptionList.stream().map(Exception::getMessage).collect(Collectors.joining("\n")));
                exceptionList.stream().forEach(e::addSuppressed);
                throw e;
            }
        }

        var actualList = selectAllFromTest();
        assertEquals(SIZE + INSERT_SIZE, actualList.size());
        TestEntity first = null;
        int i = 0;
        for (TestEntity actual : actualList) {
            if (i < SIZE) {
                var expected = createTestEntity(i);
                assertEquals(expected, actual);
            } else {
                if (first == null) {
                    first = actual;
                }
                assertEquals(first.getBar(), actual.getBar());
                assertEquals(createZzz(first.getBar(), actual.getFoo() - SIZE), actual.getZzz());
            }
            i++;
        }
    }

    private static class OnlineTask implements Callable<Void> {
        private final TsurugiSession session;
        private final TgTxOption txOption;
        private final int number;

        public OnlineTask(TsurugiSession session, TgTxOption txOption, int number) {
            this.session = session;
            this.txOption = txOption;
            this.number = number;
        }

        @Override
        public Void call() throws Exception {
            var insertSql = INSERT_SQL.replace("insert", "insert or replace");

            try (var insertPs = session.createStatement(insertSql, INSERT_MAPPING)) {
                var setting = TgTmSetting.of(txOption);
                var tm = session.createTransactionManager(setting);

                tm.execute((TsurugiTransactionAction) transaction -> {
                    for (int i = 0; i < INSERT_SIZE; i++) {
                        var entity = new TestEntity(SIZE + i, number, createZzz(number, i));
                        int count = transaction.executeAndGetCount(insertPs, entity);
                        assertUpdateCount(1, count);
                    }
                });
            }
            return null;
        }
    }

    private static String createZzz(long number, int i) {
        return String.format("%d-%02d", number, i);
    }
}
