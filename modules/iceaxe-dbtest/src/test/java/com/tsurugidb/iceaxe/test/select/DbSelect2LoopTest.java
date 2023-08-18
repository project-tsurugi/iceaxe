package com.tsurugidb.iceaxe.test.select;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.status.TgTransactionStatus;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select 2-loop test
 */
class DbSelect2LoopTest extends DbTestTableTester {
    static final Logger LOG = LoggerFactory.getLogger(DbSelect2LoopTest.class);

    private static final int SIZE = 100;
    private static final String TEST2 = "test2";

    private static final int OCC_THREAD_SIZE = 8;
    private static final int LTX_THREAD_SIZE = 2;

    private static final int LTX_ATTEMPT_SIZE = 5;

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelect2LoopTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        dropTable(TEST2);
        createTest2Table();

        logInitEnd(LOG, info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = "create table " + TEST2 + "(key1 int , key2 int, foo int, primary key(key1, key2))";
        executeDdl(getSession(), sql);
    }

    private static class Test2Entity {
        private int key1;
        private int key2;
        private int foo;

        public Test2Entity() {
        }

        public Test2Entity(int key1, int key2, int foo) {
            this.key1 = key1;
            this.key2 = key2;
            this.foo = foo;
        }

        public int getKey1() {
            return key1;
        }

        public void setKey1(int key1) {
            this.key1 = key1;
        }

        public int getKey2() {
            return key2;
        }

        public void setKey2(int key2) {
            this.key2 = key2;
        }

        public int getFoo() {
            return foo;
        }

        public void setFoo(int foo) {
            this.foo = foo;
        }
    }

    @RepeatedTest(4)
    void test() throws Throwable {
        var service = Executors.newFixedThreadPool(OCC_THREAD_SIZE + LTX_THREAD_SIZE);
        var sessionList = new ArrayList<TsurugiSession>(OCC_THREAD_SIZE + LTX_THREAD_SIZE);
        var futureList = new ArrayList<Future<Void>>(OCC_THREAD_SIZE + LTX_THREAD_SIZE);
        Throwable re = null;
        try {
            var ltxThreadCounter = new AtomicInteger(LTX_THREAD_SIZE);
            for (int i = 0; i < LTX_THREAD_SIZE; i++) {
                var session = DbTestConnector.createSession();
                sessionList.add(session);
                var future = service.submit(new LtxThread(session, i, ltxThreadCounter));
                futureList.add(future);
            }
            for (int i = 0; i < OCC_THREAD_SIZE; i++) {
                var session = DbTestConnector.createSession();
                sessionList.add(session);
                var future = service.submit(new OccThread(session, 0, ltxThreadCounter));
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

    private static class LtxThread implements Callable<Void> {
        private final TsurugiSession session;
        private final int key1;
        private final AtomicInteger threadCounter;

        public LtxThread(TsurugiSession session, int key1, AtomicInteger threadCounter) {
            this.session = session;
            this.key1 = key1;
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
            var key = TgBindVariable.ofInt("key1");
            var deleteSql = "delete from " + TEST2 + " where key1=" + key;
            var deleteMapping = TgParameterMapping.of(key);
            var insertSql = "insert into " + TEST2 + "(key1, key2, foo) values(:key1, :key2, :foo)";
            var insertMapping = TgParameterMapping.of(Test2Entity.class) //
                    .addInt("key1", Test2Entity::getKey1) //
                    .addInt("key2", Test2Entity::getKey2) //
                    .addInt("foo", Test2Entity::getFoo);
            try (var deletePs = session.createStatement(deleteSql, deleteMapping); //
                    var insertPs = session.createStatement(insertSql, insertMapping)) {
                var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX(TEST2));
                var tm = session.createTransactionManager(setting);

                for (int n = 0; n < LTX_ATTEMPT_SIZE; n++) {
                    tm.execute(transaction -> {
                        var parameter = TgBindParameters.of(key.bind(this.key1));
                        transaction.executeAndGetCount(deletePs, parameter);

                        var rcList = new ArrayList<TsurugiStatementResult>();
                        for (int i = 0; i < 100; i++) {
                            var test2 = new Test2Entity(this.key1, i, i);
                            var rc = transaction.executeStatement(insertPs, test2);
                            rcList.add(rc);
                        }
                        for (var rc : rcList) {
                            rc.close();
                        }
                    });

                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
        }
    }

    private static class OccThread implements Callable<Void> {
        private final TsurugiSession session;
        private final int key1;
        private final AtomicInteger threadCounter;

        public OccThread(TsurugiSession session, int key1, AtomicInteger threadCounter) {
            this.session = session;
            this.key1 = key1;
            this.threadCounter = threadCounter;
        }

        @Override
        public Void call() throws Exception {
            execute();
            return null;
        }

        private void execute() throws Exception {
            var key = TgBindVariable.ofInt("key1");
            var selectSql = "select * from " + TEST2 + " where key1=" + key;
            var selectMapping = TgParameterMapping.of(key);
            var selectResultMapping = TgResultMapping.of(Test2Entity::new) //
                    .addInt("key1", Test2Entity::setKey1) //
                    .addInt("key2", Test2Entity::setKey2) //
                    .addInt("foo", Test2Entity::setFoo);
            var foo = TgBindVariable.ofInt("foo");
            var select1Sql = "select * from " + TEST + " where foo=" + foo;
            var select1Mapping = TgParameterMapping.of(foo);
            try (var selectPs = session.createQuery(selectSql, selectMapping, selectResultMapping); //
                    var select1Ps = session.createQuery(select1Sql, select1Mapping)) {
                var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
                var tm = session.createTransactionManager(setting);

                while (threadCounter.get() > 0) {
                    TgTransactionStatus[] status = { null };
                    try {
                        tm.execute(transaction -> {
                            var parameter = TgBindParameters.of(key.bind(this.key1));
                            try {
                                transaction.executeAndForEach(selectPs, parameter, entity -> {
                                    var parameter1 = TgBindParameters.of(foo.bind(entity.getFoo()));
                                    transaction.executeAndGetList(select1Ps, parameter1);
                                });
                            } catch (TsurugiTransactionException e) {
                                if (e.getDiagnosticCode() == SqlServiceCode.ERR_INACTIVE_TRANSACTION) {
                                    status[0] = transaction.getTransactionStatus();
                                }
                                throw e;
                            }
                        });
                    } catch (TsurugiTmIOException e) {
                        if (e.getDiagnosticCode() == SqlServiceCode.ERR_INACTIVE_TRANSACTION) {
                            if (status[0].getDiagnosticCode() == SqlServiceCode.ERR_SERIALIZATION_FAILURE) {
//                              LOG.info("ERR_INACTIVE_TRANSACTION with ERR_SERIALIZATION_FAILURE");
                                continue;
                            }
                        }
                        throw e;
                    }
                }
            }
        }
    }
}
