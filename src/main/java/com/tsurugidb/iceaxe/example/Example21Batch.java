package com.tsurugidb.iceaxe.example;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TgTransactionOption;
import com.tsurugidb.iceaxe.transaction.TgTransactionType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * batch example
 */
public class Example21Batch {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("dbname");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            batch1(session, List.of(/* entities */));
            batch2(session, List.of(/* entities */));
        }
    }

    void batch1(TsurugiSession session, List<TestEntity> entityList) throws IOException {
        var deleteSql = "delete from TEST where FOO=:foo";
        var deleteMapping = TgParameterMapping.of(TestEntity.class) //
                .int4("foo", TestEntity::getFoo); // primary key

        var insertSql = "insert into TEST values(:foo, :bar, :zzz)";
        var insertMapping = TgParameterMapping.of(TestEntity.class) //
                .int4("foo", TestEntity::getFoo) //
                .int8("bar", TestEntity::getBar) //
                .character("zzz", TestEntity::getZzz);

        try (var deletePs = session.createPreparedStatement(deleteSql, deleteMapping); //
                var insertPs = session.createPreparedStatement(insertSql, insertMapping)) {

            var option = TgTransactionOption.of(TgTransactionType.BATCH_READ_WRITE).addWritePreserveTable("TEST");
            var tm = session.createTransactionManager(List.of(option));

            tm.execute(transaction -> {
                for (var entity : entityList) {
                    deletePs.executeAndGetCount(transaction, entity);
                    insertPs.executeAndGetCount(transaction, entity);
                }
            });
        }
    }

    void batch2(TsurugiSession session, List<TestEntity> entityList) throws IOException {
        try (var batch = new Batch2(session)) {
            batch.execute(entityList);
        }
    }

    static class Batch2 implements Closeable {

        private final TsurugiPreparedStatementUpdate1<TestEntity> deletePs;
        private final TsurugiPreparedStatementUpdate1<TestEntity> insertPs;
        private final TsurugiTransactionManager transactionManager;

        public Batch2(TsurugiSession session) throws IOException {
            var deleteSql = "delete from TEST where FOO=:foo";
            var deleteMapping = TgParameterMapping.of(TestEntity.class) //
                    .int4("foo", TestEntity::getFoo); // primary key
            this.deletePs = session.createPreparedStatement(deleteSql, deleteMapping);

            var insertSql = "insert into TEST values(:foo, :bar, :zzz)";
            var insertMapping = TgParameterMapping.of(TestEntity.class) //
                    .int4("foo", TestEntity::getFoo) //
                    .int8("bar", TestEntity::getBar) //
                    .character("zzz", TestEntity::getZzz);
            this.insertPs = session.createPreparedStatement(insertSql, insertMapping);

            var option = TgTransactionOption.of(TgTransactionType.BATCH_READ_WRITE).addWritePreserveTable("TEST");
            this.transactionManager = session.createTransactionManager(List.of(option));
        }

        public void execute(List<TestEntity> entityList) throws IOException {
            transactionManager.execute(transaction -> {
                for (var entity : entityList) {
                    deletePs.executeAndGetCount(transaction, entity);
                    insertPs.executeAndGetCount(transaction, entity);
                }
            });
        }

        @Override
        public void close() throws IOException {
            try (var c1 = deletePs; var c2 = insertPs) {
                // close
            }
        }
    }
}
