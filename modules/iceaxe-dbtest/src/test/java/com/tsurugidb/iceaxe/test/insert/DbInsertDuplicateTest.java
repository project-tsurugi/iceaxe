package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableString;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * insert duplicate bug test
 */
class DbInsertDuplicateTest extends DbTestTableTester {

    private static final String TEST2 = "test2";
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(10);

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(10);

        dropTable(TEST2);
        createTest2Table();

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = "create table " + TEST2 //
                + "(" //
                + "  key1 int," //
                + "  key2 int," //
                + "  zzz2 varchar(10)," //
                + "  primary key(key1, key2)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3, 5 })
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicateTest-occ1.*")
    void occ1(int threadSize) throws Exception {
        test(TgTxOption.ofOCC(), threadSize, false);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicateTest-occ.*")
    void occ() throws Exception {
        test(TgTxOption.ofOCC(), 40, false);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicateTest-occ.*")
    void occDebug() throws Exception {
        test(TgTxOption.ofOCC(), 20, true);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3, 5 })
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicateTest-ltx1.*")
    void ltx1(int threadSize) throws Exception {
        test(TgTxOption.ofLTX(TEST, TEST2), threadSize, false);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicateTest-ltx.*")
    void ltx() throws Exception {
        test(TgTxOption.ofLTX(TEST, TEST2), 40, false);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicateTest-ltx.*")
    void ltxDebug() throws Exception {
        test(TgTxOption.ofLTX(TEST, TEST2), 20, true);
    }

    private void test(TgTxOption txOption, int threadSize, boolean debugFlag) throws Exception {
        test(txOption, new DbTestSessions(), threadSize, debugFlag);
    }

    private void test(TgTxOption txOption, DbTestSessions sessions, int threadSize, boolean debugFlag) throws Exception {
        try (sessions) {
            var onlineList = new ArrayList<OnlineTask>(threadSize);
            var stopFlag = new AtomicBoolean(false);
            for (int i = 0; i < threadSize; i++) {
                var task = new OnlineTask(i, sessions.createSession(), txOption, debugFlag, stopFlag);
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
            if (stopFlag.get()) {
                fail("inconsistent commit data");
            }
        }
    }

    private static class OnlineTask implements Callable<Void> {
        private static final Logger LOG = LoggerFactory.getLogger(OnlineTask.class);

        private static final TgBindVariableInteger vKey1 = TgBindVariable.ofInt("key1");
        private static final TgBindVariableInteger vKey2 = TgBindVariable.ofInt("key2");
        private static final TgBindVariableString vZzz2 = TgBindVariable.ofString("zzz2");

        private final int threadNumber;
        private final TsurugiSession session;
        private final TgTxOption txOption;
        private final boolean debugFlag;
        private final AtomicBoolean stopFlag;

        public OnlineTask(int threadNumber, TsurugiSession session, TgTxOption txOption, boolean debugFlag, AtomicBoolean stopFlag) {
            this.threadNumber = threadNumber;
            this.session = session;
            this.txOption = txOption;
            this.debugFlag = debugFlag;
            this.stopFlag = stopFlag;
        }

        @Override
        public Void call() throws Exception {
            var maxSql = "select max(foo) + 1 as foo from " + TEST;

            var insert2List = TgBindVariables.of(vKey1, vKey2, vZzz2);
            var insert2Sql = "insert into " + TEST2 //
                    + "(key1, key2, zzz2)" //
                    + "values(" + insert2List.getSqlNames() + ")";
            var insert2Mapping = TgParameterMapping.of(insert2List);

            var debugSelect1Sql = SELECT_SQL + " where foo >= 10 order by foo";
            var debugSelect2Sql = "select * from " + TEST2 + " order by key1, key2";

            try (var maxPs = session.createQuery(maxSql); //
                    var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var insert2Ps = session.createStatement(insert2Sql, insert2Mapping); //
                    var debugSelect1Ps = session.createQuery(debugSelect1Sql, SELECT_MAPPING); //
                    var debugSelect2Ps = session.createQuery(debugSelect2Sql)) {
                var tm = session.createTransactionManager();
                var debugTm = session.createTransactionManager(TgTmSetting.ofOccLtx( //
                        TgTxOption.ofOCC().label("debugSelect" + threadNumber), 3, //
                        TgTxOption.ofRTX().label("debugSelect" + threadNumber), 1));

                long start = System.currentTimeMillis();
                int prev = 0;
                final int ATTEMPT_SIZE = 40;
                for (int i = 0, j = 1; i < ATTEMPT_SIZE; i++, j++) {
                    if (stopFlag.get()) {
                        break;
                    }
                    if (System.currentTimeMillis() - start > TIMEOUT) {
                        LOG.error("timeout. loop {} (i={}/{})", j, i, ATTEMPT_SIZE);
                        throw new TimeoutException(MessageFormat.format("[{0}] timeout. loop {1} (i={2})", Thread.currentThread().getName(), j, i));
                    }
                    if (j % 200 == 0) {
                        if (prev == 0) {
                            LOG.info("loop {} (i={}/{})", j, i, ATTEMPT_SIZE);
                        } else {
                            LOG.info("loop {} (i={}/{} (+{}))", j, i, ATTEMPT_SIZE, i - prev);
                        }
                        prev = i;
                    }
                    try {
                        if (stopFlag.get()) {
                            break;
                        }
                        String label = String.format("thread%d-%d", threadNumber, i);
                        var setting = TgTmSetting.ofAlways(txOption.clone(label));
                        tm.execute(setting, transaction -> {
                            if (stopFlag.get()) {
                                transaction.rollback();
                                return;
                            }
                            execute(transaction, maxPs, insertPs, insert2Ps, label);
                        });

                        if (debugFlag) {
                            if (stopFlag.get()) {
                                break;
                            }
                            if (!debugExecute(label, debugTm, debugSelect1Ps, debugSelect2Ps)) {
                                stopFlag.set(true);
                                break;
                            }
                        }
                    } catch (TsurugiTmIOException e) {
                        if (TsurugiExceptionUtil.getInstance().isUniqueConstraintViolation(e)) {
//                          LOG.info("UNIQUE_CONSTRAINT_VIOLATION {}", i);
                            i--;
                            continue;
                        }
                        LOG.error("online task error: {}", e.getMessage());
                        throw e;
                    }
                }
            }
            return null;
        }

        private void execute(TsurugiTransaction transaction, TsurugiSqlQuery<TsurugiResultEntity> maxPs, TsurugiSqlPreparedStatement<TestEntity> insertPs,
                TsurugiSqlPreparedStatement<TgBindParameters> insert2Ps, String label) throws IOException, InterruptedException, TsurugiTransactionException {
            var max = transaction.executeAndFindRecord(maxPs).get();
            int foo = max.getInt("foo");

            var entity = new TestEntity(foo, foo, label);
            transaction.executeAndGetCount(insertPs, entity);

            var parameter = TgBindParameters.of(vKey1.bind(foo), vKey2.bind(foo / 2), vZzz2.bind(label));
            transaction.executeAndGetCount(insert2Ps, parameter);
        }

        private boolean debugExecute(String label, TsurugiTransactionManager tm, TsurugiSqlQuery<TestEntity> select1Ps, TsurugiSqlQuery<TsurugiResultEntity> select2Ps)
                throws IOException, InterruptedException {
            var list1 = new ArrayList<TestEntity>();
            var list2 = new ArrayList<TsurugiResultEntity>();
            tm.execute(transaction -> {
                if (stopFlag.get()) {
                    transaction.rollback();
                    return;
                }

                list1.clear();
                list2.clear();
                list1.addAll(transaction.executeAndGetList(select1Ps));
                list2.addAll(transaction.executeAndGetList(select2Ps));
            });
            for (int i = 0; i < Math.max(list1.size(), list2.size()); i++) {
                if (i >= list1.size() || i >= list2.size()) {
                    debugLog(label, list1, list2, i);
                    return false;
                }

                var entity1 = list1.get(i);
                var entity2 = list2.get(i);

                var foo = (int) entity1.getFoo();
                var key1 = entity2.getInt("key1");
                if (foo != key1) {
                    debugLog(label, list1, list2, i);
                    return false;
                }
            }
            return true;
        }

        private void debugLog(String label, List<TestEntity> list1, ArrayList<TsurugiResultEntity> list2, int startIndex) {
            if (startIndex >= 1) {
                startIndex--;
            }
            synchronized (DbInsertDuplicateTest.class) {
                for (int i = startIndex; i < list1.size(); i++) {
                    var entity = list1.get(i);
                    LOG.error("{} test[{}]:  {}", label, i, entity);
                }
                for (int i = startIndex; i < list2.size(); i++) {
                    var entity = list2.get(i);
                    LOG.error("{} test2[{}]: {}", label, i, entity);
                }
                LOG.error("{}: test.size={}, test2.size={}", label, list1.size(), list2.size());
            }
        }
    }
}
