package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
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
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
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
class DbInsertDuplicate2Test extends DbTestTableTester {

    private static final String TEST2 = "test2";
    private static final long TIMEOUT = TimeUnit.MINUTES.toMillis(4);

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

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicate2Test-occ.*")
    void occ() throws Exception {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
        test(setting, 30, 500, false);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicate2Test-occDebug.*")
    void occDebug() throws Exception {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
        test(setting, 30, 500, true);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicate2Test-ltx.*")
    void ltx() throws Exception {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX(TEST, TEST2));
        test(setting, 30, 500, false);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicate2Test-ltxDebug.*")
    void ltxDebug() throws Exception {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX(TEST, TEST2));
        test(setting, 30, 500, true);
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertDuplicate2Test-mix.*")
    void mix() throws Exception {
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), 3, TgTxOption.ofLTX(TEST, TEST2), 40);
        test(setting, 10, 500, false);
    }

    private void test(TgTmSetting setting, int threadSize, int insertSize, boolean debugFlag) throws Exception {
        try (var sessions = new DbTestSessions(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            var counter = new AtomicInteger(0);

            var onlineList = new ArrayList<OnlineTask>(threadSize);
            var stopFlag = new AtomicBoolean(false);
            for (int i = 0; i < threadSize; i++) {
                var task = new OnlineTask(i, sessions.createSession(), setting, insertSize, counter, debugFlag, stopFlag);
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
        private final TgTmSetting setting;
        private final int insertSize;
        private final AtomicInteger counter;
        private final boolean debugFlag;
        private final AtomicBoolean stopFlag;

        public OnlineTask(int threadNumber, TsurugiSession session, TgTmSetting setting, int insertSize, AtomicInteger counter, boolean debugFlag, AtomicBoolean stopFlag) {
            this.threadNumber = threadNumber;
            this.session = session;
            this.setting = setting;
            this.insertSize = insertSize;
            this.counter = counter;
            this.debugFlag = debugFlag;
            this.stopFlag = stopFlag;
        }

        @Override
        public Void call() throws Exception {
            var maxSql = "select max(foo) + 1 as foo from " + TEST;
            var maxMapping = TgResultMapping.of(record -> record.nextInt()); // x ofSingle(), nextIntOrNull()

            var insert2List = TgBindVariables.of(vKey1, vKey2, vZzz2);
            var insert2Sql = "insert into " + TEST2 //
                    + "(key1, key2, zzz2)" //
                    + "values(" + insert2List.getSqlNames() + ")";
            var insert2Mapping = TgParameterMapping.of(insert2List);

            var debugSelect1Sql = SELECT_SQL + " where foo >= 10 order by foo";
            var debugSelect2Sql = "select * from " + TEST2 + " order by key1, key2";

            try (var maxPs = session.createQuery(maxSql, maxMapping); //
                    var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var insert2Ps = session.createStatement(insert2Sql, insert2Mapping); //
                    var debugSelect1Ps = session.createQuery(debugSelect1Sql, SELECT_MAPPING); //
                    var debugSelect2Ps = session.createQuery(debugSelect2Sql)) {
                var tm = session.createTransactionManager(setting);
                var debugTm = session.createTransactionManager(TgTmSetting.ofOccLtx( //
                        TgTxOption.ofOCC().label("debugSelect" + threadNumber), 3, //
                        TgTxOption.ofRTX().label("debugSelect" + threadNumber), 1));

                long start = System.currentTimeMillis();
                for (int i = 1; counter.get() < insertSize; i++) {
                    if (stopFlag.get()) {
                        break;
                    }
                    String label = String.format("th%d-%d", threadNumber, i);
                    if (System.currentTimeMillis() - start > TIMEOUT) {
                        LOG.error("timeout. loop {} (counter={})", i, counter.get());
                        var timeoutException = new TimeoutException(MessageFormat.format("[{0}] timeout. loop {1} (counter={2})", Thread.currentThread().getName(), i, counter.get()));
                        try {
                            debugExecute("timeout " + label, debugTm, debugSelect1Ps, debugSelect2Ps);
                        } catch (Throwable e) {
                            e.addSuppressed(timeoutException);
                            throw e;
                        }
                        throw timeoutException;
                    }
                    if (i % 200 == 0) {
                        LOG.info("loop {} (counter={})", i, counter.get());
                    }
                    try {
                        if (stopFlag.get()) {
                            break;
                        }
                        tm.setTransactionOptionModifier((opt, attempt) -> opt.clone(label + "-" + attempt));
                        tm.execute(transaction -> {
                            try {
                                execute(transaction, maxPs, insertPs, insert2Ps, label);
                            } catch (TsurugiTransactionException e) {
                                if (e.getMessage().contains("USER_ABORT")) {
                                    LOG.info("USER_ABORT: {}", transaction.getTransactionStatus());
                                }
                                throw e;
                            }
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
                        var exceptionUtil = TsurugiExceptionUtil.getInstance();
                        if (exceptionUtil.isUniqueConstraintViolation(e)) {
//                          LOG.info("UNIQUE_CONSTRAINT_VIOLATION: {}", e.getMessage());
                            continue;
                        }
                        if (exceptionUtil.isSerializationFailure(e)) {
                            String message = e.getMessage();
                            if (message.contains("reason_code:KVS_INSERT")) {
//                              LOG.info("CC_EXCEPTION:KVS_INSERT: {}", message);
                                continue;
                            }
                        }
                        LOG.error("online task error: {}", e.getMessage());
                        throw e;
                    } catch (Throwable e) {
                        LOG.error("online task error", e);
                        throw e;
                    }
                    counter.incrementAndGet();
                }

                synchronized (OnlineTask.class) {
                    if (!stopFlag.get()) {
                        String label = String.format("th%d-endCheck", threadNumber);
                        if (!debugExecute(label, debugTm, debugSelect1Ps, debugSelect2Ps)) {
                            stopFlag.set(true);
                        }
                    }
                }
            }
            return null;
        }

        private void execute(TsurugiTransaction transaction, TsurugiSqlQuery<Integer> maxPs, TsurugiSqlPreparedStatement<TestEntity> insertPs, TsurugiSqlPreparedStatement<TgBindParameters> insert2Ps,
                String label) throws IOException, InterruptedException, TsurugiTransactionException {
            int foo = transaction.executeAndFindRecord(maxPs).get();

            var entity = new TestEntity(foo, foo, label);
            int count1 = transaction.executeAndGetCount(insertPs, entity);
            assertEquals(1, count1);

            for (int i = 0; i < 10; i++) {
                var parameter = TgBindParameters.of(vKey1.bind(foo), vKey2.bind(i + 1), vZzz2.bind(label));
                int count2 = transaction.executeAndGetCount(insert2Ps, parameter);
                assertEquals(1, count2);
            }
        }

        private boolean debugExecute(String label, TsurugiTransactionManager tm, TsurugiSqlQuery<TestEntity> select1Ps, TsurugiSqlQuery<TsurugiResultEntity> select2Ps)
                throws IOException, InterruptedException {
            var list1 = new ArrayList<TestEntity>();
            var list2 = new ArrayList<TsurugiResultEntity>();
            TgTxOption[] debugTxOption = { null };
            tm.execute(transaction -> {
                list1.clear();
                list2.clear();
                if (stopFlag.get()) {
                    transaction.rollback();
                    return;
                }

                list1.addAll(transaction.executeAndGetList(select1Ps));
                list2.addAll(transaction.executeAndGetList(select2Ps));
                debugTxOption[0] = transaction.getTransactionOption();
            });

            var map = new TreeMap<Integer, Count>();
            {
                int i = 0;
                for (var entity1 : list1) {
                    var c = map.computeIfAbsent(entity1.getFoo(), Count::new);
                    c.index1 = i++;
                    c.count1++;
                    c.zzz1 = entity1.getZzz();
                }
                i = 0;
                for (var entity2 : list2) {
                    var c = map.computeIfAbsent(entity2.getInt("key1"), Count::new);
                    if (c.index2 < 0) {
                        c.index2 = i;
                    }
                    i++;
                    c.count2++;
                    c.zzz2Set.add(entity2.getString("zzz2"));
                }
            }

            var invalidList = map.values().stream().filter(c -> c.invalid()).collect(Collectors.toList());
            if (!invalidList.isEmpty()) {
                debugLog(label, list1, list2, invalidList, debugTxOption[0]);
                return false;
            }
            return true;
        }

        private static class Count {
            private final int key1;
            int index1 = -1;
            int count1 = 0;
            int index2 = -1;
            String zzz1;
            int count2 = 0;
            final TreeSet<String> zzz2Set = new TreeSet<>();

            public Count(int key1) {
                this.key1 = key1;
            }

            public boolean invalid() {
                if (count1 != 1 || count2 != 10 || zzz2Set.size() != 1) {
                    return true;
                }
                String zzz2 = zzz2Set.first();
                if (!zzz1.equals(zzz2)) {
                    return true;
                }
                return false;
            }

            @Override
            public String toString() {
                return "key1=" + key1 + ", count1=" + count1 + ", zzz1=" + zzz1 + ", count2=" + count2 + ", zzz2=" + zzz2Set;
            }
        }

        private void debugLog(String label, List<TestEntity> list1, List<TsurugiResultEntity> list2, List<Count> invalidList, TgTxOption txOption) {
            var first = invalidList.get(0);
            int startIndex1 = first.index1;
            if (startIndex1 >= 1) {
                startIndex1--;
            }
            if (startIndex1 < 0) {
                startIndex1 = 0;
            }
            int startIndex2 = first.index2;
            if (startIndex2 >= 1) {
                startIndex2--;
            }
            if (startIndex2 < 0) {
                startIndex2 = 0;
            }
            synchronized (DbInsertDuplicateTest.class) {
                for (int i = startIndex1; i < list1.size(); i++) {
                    var entity = list1.get(i);
                    LOG.error("{} test[{}]:  {}", label, i, entity);
                }
                for (int i = startIndex2; i < list2.size(); i++) {
                    var entity = list2.get(i);
                    LOG.error("{} test2[{}]: {}", label, i, entity);
                }
                LOG.error("{}: test.size={}, test2.size={}, readTxOption={}", label, list1.size(), list2.size(), txOption);

                for (var c : invalidList) {
                    LOG.error("{} error data: {}", label, c);
                }
            }
        }
    }
}
