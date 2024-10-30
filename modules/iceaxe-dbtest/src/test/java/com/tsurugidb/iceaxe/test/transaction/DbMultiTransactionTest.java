package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * multi transaction test
 */
class DbMultiTransactionTest extends DbTestTableTester {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;
    private static final int THREAD_SIZE = ResponseBox.responseBoxSize() + 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(THREAD_SIZE);

        logInitEnd(info);
    }

    @RepeatedTest(6)
    void doNothing() throws Exception {
        try (var session = DbTestConnector.createSession()) {
            for (int i = 0; i < ATTEMPT_SIZE; i++) {
                try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                    // do nothing
                }
            }
        }

        try (var session = DbTestConnector.createSession()) {
            var tm = session.createTransactionManager();
            tm.executeDdl("drop table if exists test");
        }
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 200 })
    void updateMultiThread(long wait) throws Exception {
        var service = Executors.newCachedThreadPool();
        try {
            var futureList = new ArrayList<Future<Void>>(THREAD_SIZE);

            var session = getSession();
            var stopFlag = new AtomicBoolean(false);
            for (int i = 0; i < THREAD_SIZE; i++) {
                var task = new UpdateThread(session, i, wait, stopFlag);
                futureList.add(service.submit(task));
            }

            Exception exception = null;
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception e) {
                exception = e;
            } finally {
                stopFlag.set(true);
            }

            for (var future : futureList) {
                try {
                    future.get();
                } catch (Exception e) {
                    if (exception == null) {
                        exception = e;
                    } else {
                        exception.addSuppressed(e);
                    }
                }
            }
            if (exception != null) {
                throw exception;
            }
        } finally {
            service.shutdownNow();
        }
    }

    private static class UpdateThread implements Callable<Void> {

        private final TsurugiSession session;
        private final int number;
        private final long wait;
        private final AtomicBoolean stopFlag;

        public UpdateThread(TsurugiSession session, int number, long wait, AtomicBoolean stopFlag) {
            this.session = session;
            this.number = number;
            this.wait = wait;
            this.stopFlag = stopFlag;
        }

        @Override
        public Void call() throws Exception {
            var foo = TgBindVariable.ofInt("foo");
            var bar = TgBindVariable.ofLong("bar");

            var selectSql = SELECT_SQL + " where foo=" + foo;
            var selectMapping = TgParameterMapping.of(foo);
            var updateSql = "update " + TEST + " set bar=" + bar + " where foo=" + foo;
            var updateMapping = TgParameterMapping.of(foo, bar);

            try (var selectPs = session.createQuery(selectSql, selectMapping, SELECT_MAPPING); //
                    var updatePs = session.createStatement(updateSql, updateMapping)) {
                var tm = createTransactionManagerOcc(session);
                while (!stopFlag.get()) {
                    tm.execute(transaction -> {
                        TestEntity entity;
                        var selectParameter = TgBindParameters.of(foo.bind(number));
                        try (var selectResult = transaction.executeQuery(selectPs, selectParameter)) {
                            if (wait > 0) {
                                TimeUnit.MILLISECONDS.sleep(wait);
                            }
                            var list = selectResult.getRecordList();
                            assertEquals(1, list.size());
                            entity = list.get(0);
                        }

                        var updateParameter = TgBindParameters.of(foo.bind(number), bar.bind(entity.getBar() + 1));
                        try (var updateResult = transaction.executeStatement(updatePs, updateParameter)) {
                            if (wait > 0) {
                                TimeUnit.MILLISECONDS.sleep(wait);
                            }
                            int count = updateResult.getUpdateCount();
                            assertEquals(1, count);
                        }
                    });
                }
            }

            return null;
        }
    }
}
