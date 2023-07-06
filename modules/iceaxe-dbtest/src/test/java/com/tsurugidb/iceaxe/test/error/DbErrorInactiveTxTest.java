package com.tsurugidb.iceaxe.test.error;

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

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * ERR_INACTIVE_TRANSACTION bug test
 */
class DbErrorInactiveTxTest extends DbTestTableTester {

    private static final int TEST_SIZE = 10;
    private static final String TEST2 = "test2";

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(TEST_SIZE);

        dropTable(TEST2);
        createTest2Table();

        logInitEnd(info);
    }

    private static void createTest2Table() throws IOException, InterruptedException {
        var sql = "create table " + TEST2 //
                + "(" //
                + "  group_id int," //
                + "  id int," //
                + "  fk_foo int," //
                + "  value1 decimal(10,2)," //
                + "  value2 decimal(10,2)," //
                + "  primary key(group_id, id)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @Test
    void test() throws Exception {
        int batchSize = 10;
        int onlineSize = 20;

        try (var sessions = new DbTestSessions()) {
            var batchList = new ArrayList<BatchTask>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                var task = new BatchTask(sessions.createSession(), i);
                batchList.add(task);
            }
            var onlineList = new ArrayList<OnlineTask>(onlineSize);
            for (int i = 0; i < onlineSize; i++) {
                var task = new OnlineTask(sessions.createSession(), i);
                onlineList.add(task);
            }

            var service = Executors.newCachedThreadPool();
            var futureList = new ArrayList<Future<?>>(batchSize + onlineSize);
            onlineList.forEach(task -> futureList.add(service.submit(task)));
            batchList.forEach(task -> futureList.add(service.submit(task)));

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

    private static class BatchTask implements Callable<Void> {
        private final TsurugiSession session;
        private final int targetGroupId;

        public BatchTask(TsurugiSession session, int groupId) {
            this.session = session;
            this.targetGroupId = groupId;
        }

        @Override
        public Void call() throws Exception {
            var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX(TEST2));
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                execute(transaction);
            });
            return null;
        }

        private void execute(TsurugiTransaction transaction) throws IOException, InterruptedException, TsurugiTransactionException {
            var groupId = TgBindVariable.ofInt("groupId");
            var id = TgBindVariable.ofInt("id");
            var fkFoo = TgBindVariable.ofInt("fkFoo");
            var value1 = TgBindVariable.ofDecimal("value1");
            var value2 = TgBindVariable.ofDecimal("value2");
            var vlist = TgBindVariables.of(groupId, id, fkFoo, value1, value2);
            var sql = "insert into " + TEST2 //
                    + "(group_id, id, fk_foo, value1, value2)" //
                    + "values(" + vlist.getSqlNames() + ")";
            var parameterMapping = TgParameterMapping.of(vlist);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                for (int i = 0; i < 50; i++) {
                    var parameter = TgBindParameters.of( //
                            groupId.bind(targetGroupId), //
                            id.bind(i + 1), //
                            fkFoo.bind(i % TEST_SIZE), //
                            value1.bind(i + 100), //
                            value2.bind(i + 200) //
                    );
                    transaction.executeAndGetCount(ps, parameter);
                }
            }
        }
    }

    private static class OnlineTask implements Callable<Void> {
        private static final Logger LOG = LoggerFactory.getLogger(OnlineTask.class);

        private final TsurugiSession session;
        private final int maxGroupId;
        private final TgBindVariableInteger vGruopId = TgBindVariable.ofInt("groupId");

        public OnlineTask(TsurugiSession session, int groupId) {
            this.session = session;
            this.maxGroupId = groupId;
        }

        @Override
        public Void call() throws Exception {
            var sql = "select group_id, id, fk_foo, value1, value2\n" //
                    + "from " + TEST2 + "\n" //
                    + "inner join " + TEST + " t1 on t1.foo=fk_foo\n" //
                    + "where group_id=" + vGruopId;
            var parameterMapping = TgParameterMapping.of(vGruopId);
            try (var ps = session.createQuery(sql, parameterMapping)) {
                var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
                var tm = session.createTransactionManager(setting);
                tm.addEventListener(new TsurugiTmEventListener() {
                    @Override
                    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTmTxOption nextTmOption) {
//                      LOG.info("OCC retry: " + cause.getMessage());
                    }
                });

                for (int i = 0; i < maxGroupId; i++) {
                    int groupId = i;
                    try {
                        tm.execute(transaction -> {
                            execute(transaction, ps, groupId);
                        });
                    } catch (TsurugiTmIOException e) {
                        LOG.error("online task error: {}", e.getMessage());
                        throw e;
                    }
                }
            }
            return null;
        }

        private void execute(TsurugiTransaction transaction, TsurugiSqlPreparedQuery<TgBindParameters, TsurugiResultEntity> ps, int groupId)
                throws IOException, InterruptedException, TsurugiTransactionException {
            var parameter = TgBindParameters.of(vGruopId.bind(groupId));
            transaction.executeAndGetList(ps, parameter);
        }
    }
}
