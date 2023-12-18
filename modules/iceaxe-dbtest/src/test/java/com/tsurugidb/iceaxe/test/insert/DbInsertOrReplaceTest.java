package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * insert or replace test
 */
class DbInsertOrReplaceTest extends DbTestTableTester {

    private static final int SIZE = 10;
    private static final int INSERT_SIZE = 20;
    private static final String UPSERT_SQL = INSERT_SQL.replace("insert", "insert or replace");

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(ints = { 2, 10 })
    void occ(int threadSize) throws Exception {
        test(TgTxOption.ofOCC(), threadSize);
    }

    @RepeatedTest(100)
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertOrReplaceTest-ltx.*")
    void ltx() throws Exception {
        test(TgTxOption.ofLTX(TEST), 10);
    }

    @RepeatedTest(10)
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertOrReplaceTest-ltx.*")
    void ltx_2() throws Exception {
        test(TgTxOption.ofLTX(TEST), 2);
    }

    private void test(TgTxOption txOption, int threadSize) throws Exception {
        test(txOption, new DbTestSessions(), threadSize);
    }

    private void test(TgTxOption txOption, DbTestSessions sessions, int threadSize) throws Exception {
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
        try {
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
                    try {
                        assertEquals(first.getBar(), actual.getBar());
                        assertEquals(createZzz(first.getBar(), actual.getFoo() - SIZE), actual.getZzz());
                    } catch (AssertionError e) {
                        LOG.error("i={}, first={}, actual={}", i, first, actual, e);
                        throw e;
                    }
                }
                i++;
            }
        } catch (AssertionError e) {
            int i = 0;
            for (var actual : actualList) {
                LOG.error("actual[{}]={}", i, actual);
                i++;
            }
            throw e;
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
            try (var insertPs = session.createStatement(UPSERT_SQL, INSERT_MAPPING)) {
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

    @Test
    void occ2() throws Exception {
        test2(TgTxOption.ofOCC(), 2);
    }

    @Test
    void ltx2() throws Exception {
        test2(TgTxOption.ofLTX(TEST), 1);
    }

    private void test2(TgTxOption txOption, long expectedBar) throws Exception {
        var session = getSession();
        try (var insertPs = session.createStatement(UPSERT_SQL, INSERT_MAPPING); //
                var tx1 = session.createTransaction(txOption); //
                var tx2 = session.createTransaction(txOption)) {
            int N1 = 1;
            int N2 = 2;

            int i = 0;
            var entity11 = new TestEntity(SIZE + i, N1, createZzz(N1, i));
            tx1.executeAndGetCount(insertPs, entity11);
            var entity21 = new TestEntity(SIZE + i, N2, createZzz(N2, i));
            tx2.executeAndGetCount(insertPs, entity21);

            i++;
            var entity22 = new TestEntity(SIZE + i, N2, createZzz(N2, i));
            tx2.executeAndGetCount(insertPs, entity22);
            var entity12 = new TestEntity(SIZE + i, N1, createZzz(N1, i));
            tx1.executeAndGetCount(insertPs, entity12);

            tx1.commit(TgCommitType.DEFAULT);
//          assert2(1); // TODO tx1コミット後にselectして状態を確認したい
            tx2.commit(TgCommitType.DEFAULT);
        }

        assert2(expectedBar);
    }

    private void assert2(long expectedBar) throws IOException, InterruptedException {
        var actualList = selectAllFromTest();
        try {
            int i = 0;
            for (TestEntity actual : actualList) {
                if (i < SIZE) {
                    var expected = createTestEntity(i);
                    assertEquals(expected, actual);
                } else {
                    assertEquals(expectedBar, actual.getBar());
                }
                i++;
            }
        } catch (AssertionError e) {
            int i = 0;
            for (var actual : actualList) {
                LOG.error("actual[{}]={}", i, actual);
                i++;
            }
            throw e;
        }
    }
}
