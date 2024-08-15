package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * batch application example
 */
public class Example81Batch {

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            Example11Ddl.dropAndCreateTable(session);
            Example21Insert.insert(session);

            new Example81Batch().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        var entityList = List.of( //
                new TestEntity(122, 122L, "new122"), //
                new TestEntity(124, 124L, "new124"), //
                new TestEntity(126, 126L, "new126"));
        batch1(session, entityList);
        batch2(session, entityList);
    }

    void batch1(TsurugiSession session, List<TestEntity> entityList) throws IOException, InterruptedException {
        var deleteSql = "delete from TEST where FOO=:foo";
        var deleteMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo); // primary key

        var insertSql = "insert into TEST values(:foo, :bar, :zzz)";
        var insertMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);

        try (var deletePs = session.createStatement(deleteSql, deleteMapping); //
                var insertPs = session.createStatement(insertSql, insertMapping)) {

            var setting = TgTmSetting.of(TgTxOption.ofLTX("TEST"));
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                for (var entity : entityList) {
                    transaction.executeAndGetCount(deletePs, entity);
                    transaction.executeAndGetCount(insertPs, entity);
                }
            });
        }
    }

    void batch2(TsurugiSession session, List<TestEntity> entityList) throws IOException, InterruptedException {
        try (var batch = new Batch2(session)) {
            batch.execute(entityList);
        }
    }

    static class Batch2 implements AutoCloseable {

        private final TsurugiSqlPreparedStatement<TestEntity> deletePs;
        private final TsurugiSqlPreparedStatement<TestEntity> insertPs;
        private final TsurugiTransactionManager transactionManager;

        public Batch2(TsurugiSession session) throws IOException, InterruptedException {
            var deleteSql = "delete from TEST where FOO=:foo";
            var deleteMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("foo", TestEntity::getFoo); // primary key
            this.deletePs = session.createStatement(deleteSql, deleteMapping);

            var insertSql = "insert into TEST values(:foo, :bar, :zzz)";
            var insertMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("foo", TestEntity::getFoo) //
                    .addLong("bar", TestEntity::getBar) //
                    .addString("zzz", TestEntity::getZzz);
            this.insertPs = session.createStatement(insertSql, insertMapping);

            var setting = TgTmSetting.of(TgTxOption.ofLTX("TEST"));
            this.transactionManager = session.createTransactionManager(setting);
        }

        public void execute(List<TestEntity> entityList) throws IOException, InterruptedException {
            transactionManager.execute(transaction -> {
                for (var entity : entityList) {
                    transaction.executeAndGetCount(deletePs, entity);
                    transaction.executeAndGetCount(insertPs, entity);
                }
            });
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try (deletePs; insertPs) {
                // close
            }
        }
    }
}
