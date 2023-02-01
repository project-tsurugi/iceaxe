package com.tsurugidb.iceaxe.test.error;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.event.TgTmEventListener;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * ERR_INACTIVE_TRANSACTION bug test
 */
class DbErrorInactiveTxTest extends DbTestTableTester {

    private static final int TEST_SIZE = 10;
    private static final String TEST2 = "test2";

    @BeforeEach
    void beforeEach() throws IOException {
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(TEST_SIZE);

        dropTable(TEST2);
        createTest2Table();

        LOG.debug("init end");
    }

    private static void createTest2Table() throws IOException {
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
    @Disabled // TODO remove Disabled: ERR_INACTIVE_TRANSACTIONが発生しなくなったらDisabledを削除
    void test() throws IOException, InterruptedException {
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

        private void execute(TsurugiTransaction transaction) throws IOException, TsurugiTransactionException {
            var groupId = TgVariable.ofInt4("groupId");
            var id = TgVariable.ofInt4("id");
            var fkFoo = TgVariable.ofInt4("fkFoo");
            var value1 = TgVariable.ofDecimal("value1");
            var value2 = TgVariable.ofDecimal("value2");
            var vlist = List.of(groupId, id, fkFoo, value1, value2);
            var sql = "insert into " + TEST2 //
                    + "(group_id, id, fk_foo, value1, value2)" //
                    + "values(" + vlist.stream().map(v -> v.sqlName()).collect(Collectors.joining(",")) + ")";
            var parameterMapping = TgParameterMapping.of(vlist);
            try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
                for (int i = 0; i < 50; i++) {
                    var parameter = TgParameterList.of( //
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
        private final TgVariableInteger vGruopId = TgVariable.ofInt4("groupId");

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
            try (var ps = session.createPreparedQuery(sql, parameterMapping)) {
                var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
                var tm = session.createTransactionManager(setting);
                tm.addEventListener(new TgTmEventListener() {
                    @Override
                    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextOption) {
//                      LOG.info("OCC retry: " + cause.getMessage());
                    }
                });

                for (int i = 0; i < maxGroupId; i++) {
                    int groupId = i;
                    try {
                        tm.execute(transaction -> {
                            execute(transaction, ps, groupId);
                        });
                    } catch (TsurugiTransactionIOException e) {
                        LOG.error("online task error: {}", e.getMessage());
                        throw e;
                    }
                }
            }
            return null;
        }

        private void execute(TsurugiTransaction transaction, TsurugiPreparedStatementQuery1<TgParameterList, TsurugiResultEntity> ps, int groupId) throws IOException, TsurugiTransactionException {
            var parameter = TgParameterList.of(vGruopId.bind(groupId));
            transaction.executeAndGetList(ps, parameter);
        }
    }
}
