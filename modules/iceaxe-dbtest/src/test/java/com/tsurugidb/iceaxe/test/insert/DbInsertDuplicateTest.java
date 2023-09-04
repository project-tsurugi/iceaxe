package com.tsurugidb.iceaxe.test.insert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
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
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * insert duplicate bug test
 */
class DbInsertDuplicateTest extends DbTestTableTester {

    private static final String TEST2 = "test2";

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
    void occ() throws Exception {
        test(TgTxOption.ofOCC());
    }

    @Test
    void ltx() throws Exception {
        test(TgTxOption.ofLTX(TEST, TEST2));
    }

    private void test(TgTxOption txOption) throws Exception {
        test(txOption, new DbTestSessions());
    }

    private void test(TgTxOption txOption, DbTestSessions sessions) throws Exception {
        int onlineSize = 40;

        try (sessions) {
            var onlineList = new ArrayList<OnlineTask>(onlineSize);
            for (int i = 0; i < onlineSize; i++) {
                var task = new OnlineTask(sessions.createSession(), txOption);
                onlineList.add(task);
            }

            var service = Executors.newCachedThreadPool();
            var futureList = new ArrayList<Future<?>>(onlineSize);
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
    }

    private static class OnlineTask implements Callable<Void> {
        private static final Logger LOG = LoggerFactory.getLogger(OnlineTask.class);

        private static final TgBindVariableInteger vKey1 = TgBindVariable.ofInt("key1");
        private static final TgBindVariableInteger vKey2 = TgBindVariable.ofInt("key2");
        private static final TgBindVariableString vZzz2 = TgBindVariable.ofString("zzz2");

        private final TsurugiSession session;
        private final TgTxOption txOption;

        public OnlineTask(TsurugiSession session, TgTxOption txOption) {
            this.session = session;
            this.txOption = txOption;
        }

        @Override
        public Void call() throws Exception {
            var maxSql = "select max(foo) + 1 as foo from " + TEST;

            var insert2List = TgBindVariables.of(vKey1, vKey2, vZzz2);
            var insert2Sql = "insert into " + TEST2 //
                    + "(key1, key2, zzz2)" //
                    + "values(" + insert2List.getSqlNames() + ")";
            var insert2Mapping = TgParameterMapping.of(insert2List);

            try (var maxPs = session.createQuery(maxSql); //
                    var insertPs = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var insert2Ps = session.createStatement(insert2Sql, insert2Mapping)) {
                var setting = TgTmSetting.ofAlways(txOption);
                var tm = session.createTransactionManager(setting);

                for (int i = 0; i < 40; i++) {
                    try {
                        tm.execute(transaction -> {
                            execute(transaction, maxPs, insertPs, insert2Ps);
                        });
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
                TsurugiSqlPreparedStatement<TgBindParameters> insert2Ps) throws IOException, InterruptedException, TsurugiTransactionException {
            var max = transaction.executeAndFindRecord(maxPs).get();
            int foo = max.getInt("foo");

            var entity = new TestEntity(foo, foo, Integer.toString(foo));
            transaction.executeAndGetCount(insertPs, entity);

            var parameter = TgBindParameters.of(vKey1.bind(foo), vKey2.bind(foo / 2), vZzz2.bind(Integer.toString(foo)));
            transaction.executeAndGetCount(insert2Ps, parameter);
        }
    }
}
