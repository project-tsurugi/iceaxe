package com.tsurugidb.iceaxe.test.select;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * select while delete-insert test
 */
class DbSelectWhileInsertTest extends DbTestTableTester {

    private static final int SIZE = 100;

    private static final int INSERT_THREAD_SIZE = 1;
    private static final int SELECT_THREAD_SIZE = 6;

    private static final int INSERT_ATTEMPT_SIZE = 4;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectWhileInsertTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @RepeatedTest(3)
    void test() throws Throwable {
        var service = Executors.newFixedThreadPool(INSERT_THREAD_SIZE + SELECT_THREAD_SIZE);
        var sessionList = new ArrayList<TsurugiSession>(INSERT_THREAD_SIZE + SELECT_THREAD_SIZE);
        var futureList = new ArrayList<Future<Void>>(INSERT_THREAD_SIZE + SELECT_THREAD_SIZE);
        Throwable re = null;
        try {
            var ltxThreadCounter = new AtomicInteger(INSERT_THREAD_SIZE);
            for (int i = 0; i < INSERT_THREAD_SIZE; i++) {
                var session = DbTestConnector.createSession();
                sessionList.add(session);
                var future = service.submit(new InsertThread(session, ltxThreadCounter));
                futureList.add(future);
            }
            for (int i = 0; i < SELECT_THREAD_SIZE; i++) {
                var session = DbTestConnector.createSession();
                sessionList.add(session);
                var future = service.submit(new SelectThread(session, ltxThreadCounter));
                futureList.add(future);
            }
        } catch (Throwable e) {
            re = e;
            throw e;
        } finally {
            for (var future : futureList) {
                try {
                    future.get();
                    // test success if no error
                } catch (Exception e) {
                    if (re == null) {
                        re = e;
                    } else {
                        re.addSuppressed(e);
                    }
                }
            }
            for (var session : sessionList) {
                try {
                    session.close();
                } catch (Exception e) {
                    if (re == null) {
                        re = e;
                    } else {
                        re.addSuppressed(e);
                    }
                }
            }
            if (re != null) {
                throw re;
            }
        }
    }

    private static class InsertThread implements Callable<Void> {
        private final TsurugiSession session;
        private final AtomicInteger threadCounter;

        public InsertThread(TsurugiSession session, AtomicInteger threadCounter) {
            this.session = session;
            this.threadCounter = threadCounter;
        }

        @Override
        public Void call() throws Exception {
            try {
                execute();
            } finally {
                threadCounter.decrementAndGet();
            }
            return null;
        }

        private void execute() throws Exception {
            var key = TgBindVariable.ofInt("foo");
            var deleteSql = "delete from " + TEST + " where foo=" + key;
            var deleteMapping = TgParameterMapping.of(key);
            try (var deletePs = session.createStatement(deleteSql, deleteMapping); //
                    var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
                var tm = session.createTransactionManager(setting);

                for (int n = 0; n < INSERT_ATTEMPT_SIZE; n++) {
                    for (int i = 0; i < SIZE; i++) {
                        int foo = i;
                        tm.execute(transaction -> {
                            var parameter = TgBindParameters.of(key.bind(foo));
                            transaction.executeAndGetCount(deletePs, parameter);
                        });
                        tm.execute(transaction -> {
                            var parameter = createTestEntity(foo);
                            transaction.executeAndGetCount(insertPs, parameter);
                        });
                    }
                }
            }
        }
    }

    private static class SelectThread implements Callable<Void> {
        private final TsurugiSession session;
        private final AtomicInteger threadCounter;

        public SelectThread(TsurugiSession session, AtomicInteger threadCounter) {
            this.session = session;
            this.threadCounter = threadCounter;
        }

        @Override
        public Void call() throws Exception {
            execute();
            return null;
        }

        private void execute() throws Exception {
            try (var selectPs = session.createQuery(SELECT_SQL, TgParameterMapping.of(), SELECT_MAPPING)) {
                var setting = TgTmSetting.ofAlways(TgTxOption.ofRTX());
                var tm = session.createTransactionManager(setting);

                while (threadCounter.get() > 0) {
                    tm.execute(transaction -> {
                        transaction.executeAndGetList(selectPs, null);
                    });
                }
            }
        }
    }
}
