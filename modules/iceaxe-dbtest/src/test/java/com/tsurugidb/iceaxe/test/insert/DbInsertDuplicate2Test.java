package com.tsurugidb.iceaxe.test.insert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableString;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert duplicate bug test
 */
class DbInsertDuplicate2Test extends DbTestTableTester {

    private static final String TEST2 = "test2";

    @BeforeEach
    void beforeEach() throws IOException {
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(10);

        dropTable(TEST2);
        createTest2Table();

        LOG.debug("init end");
    }

    private static void createTest2Table() throws IOException {
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
    void occ() throws Exception {
        test(TgTxOption.ofOCC(), 30, 500);
    }

    @Test
    void ltx() throws Exception {
        test(TgTxOption.ofLTX(TEST, TEST2), 30, 500);
    }

    private void test(TgTxOption txOption, int threadSize, int attemptSize) throws Exception {
        try (var sessions = new DbTestSessions(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            var counter = new AtomicInteger(0);

            var onlineList = new ArrayList<OnlineTask>(threadSize);
            for (int i = 0; i < threadSize; i++) {
                var task = new OnlineTask(sessions.createSession(), txOption, attemptSize, counter);
                onlineList.add(task);
            }

            var service = Executors.newCachedThreadPool();
            var futureList = new ArrayList<Future<?>>(threadSize);
            onlineList.forEach(task -> futureList.add(service.submit(task)));

            RuntimeException save = null;
            for (var future : futureList) {
                try {
                    future.get();
                } catch (Exception e) {
                    if (save == null) {
                        save = new RuntimeException("future exception");
                    }
                    save.addSuppressed(e);
                }
            }
            if (save != null) {
                throw save;
            }
        }
    }

    private static class OnlineTask implements Callable<Void> {
        private static final Logger LOG = LoggerFactory.getLogger(OnlineTask.class);

        private static final TgBindVariableInteger vKey1 = TgBindVariable.ofInt("key1");
        private static final TgBindVariableInteger vKey2 = TgBindVariable.ofInt("key2");
        private static final TgBindVariableString vZzz2 = TgBindVariable.ofString("zzz2");

        private final TsurugiSession session;
        private final TgTxOption txOption;
        private final int attemptSize;
        private final AtomicInteger counter;

        public OnlineTask(TsurugiSession session, TgTxOption txOption, int attemptSize, AtomicInteger counter) {
            this.session = session;
            this.txOption = txOption;
            this.attemptSize = attemptSize;
            this.counter = counter;
        }

        @Override
        public Void call() throws Exception {
            var maxSql = "select max(foo) + 1 as foo from " + TEST;

            var insert2List = List.of(vKey1, vKey2, vZzz2);
            var insert2Sql = "insert into " + TEST2 //
                    + "(key1, key2, zzz2)" //
                    + "values(" + insert2List.stream().map(v -> v.sqlName()).collect(Collectors.joining(", ")) + ")";
            var insert2Mapping = TgParameterMapping.of(insert2List);

            try (var maxPs = session.createQuery(maxSql); //
                    var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var insert2Ps = session.createStatement(insert2Sql, insert2Mapping)) {
                var setting = TgTmSetting.ofAlways(txOption);
                var tm = session.createTransactionManager(setting);

                for (;;) {
                    if (counter.get() >= attemptSize) {
                        break;
                    }
                    try {
                        tm.execute(transaction -> {
                            execute(transaction, maxPs, insertPs, insert2Ps);
                        });
                    } catch (TsurugiTransactionIOException e) {
                        if (e.getDiagnosticCode() == SqlServiceCode.ERR_ALREADY_EXISTS) {
//                          LOG.info("ERR_ALREADY_EXISTS {}", i);
                            continue;
                        }
                        LOG.error("online task error: {}", e.getMessage());
                        throw e;
                    }
                    counter.incrementAndGet();
                }
            }
            return null;
        }

        private void execute(TsurugiTransaction transaction, TsurugiSqlQuery<TsurugiResultEntity> maxPs, TsurugiSqlPreparedStatement<TestEntity> insertPs,
                TsurugiSqlPreparedStatement<TgBindParameters> insert2Ps) throws IOException, TsurugiTransactionException {
            var max = transaction.executeAndFindRecord(maxPs).get();
            int foo = max.getInt("foo");

            var entity = new TestEntity(foo, foo, Integer.toString(foo));
            transaction.executeAndGetCount(insertPs, entity);

            for (int i = 0; i < 10; i++) {
                var parameter = TgBindParameters.of(vKey1.bind(foo), vKey2.bind(i + 1), vZzz2.bind(Integer.toString(foo)));
                transaction.executeAndGetCount(insert2Ps, parameter);
            }
        }
    }
}
