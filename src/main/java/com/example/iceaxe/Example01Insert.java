package com.example.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.RecursiveAction;

import com.tsurugi.iceaxe.TsurugiConnector;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.statement.TgDataType;
import com.tsurugi.iceaxe.statement.TgParameterList;
import com.tsurugi.iceaxe.statement.TgParameterMapping;
import com.tsurugi.iceaxe.statement.TgVariableList;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionIOException;
import com.tsurugi.iceaxe.transaction.TsurugiTransactionUncheckedIOException;

/**
 * insert example
 */
public class Example01Insert {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("dbname");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            insert0(session);
            insertParameter(session);
            insertEntity(session);
            insertEntityMapping(session);
            insertForkJoin(session, List.of(/* entities */));
        }
    }

    void insert0(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));

        try (var ps = session.createPreparedStatement("insert into TEST values(123, 456, 'abc')")) {
            tm.execute(transaction -> {
                // FIXME ps.execute毎にtryで囲むのはうざい？
                try (var result = ps.execute(transaction)) {
                    System.out.println(result.getUpdateCount());
                }
            });
        }
    }

    void insertParameter(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));

        var sql = "insert into TEST values(:foo, :bar, :zzz)";

        TgVariableList variable;
        switch (0) {
        default:
            variable = TgVariableList.of().int4("foo").int8("bar").character("zzz");
            break;
        case 1:
            variable = TgVariableList.of().add("foo", TgDataType.INT4).add("bar", TgDataType.INT8).add("zzz", TgDataType.CHARACTER);
            break;
        case 2:
            variable = TgVariableList.of().add("foo", int.class).add("bar", long.class).add("zzz", String.class);
            break;
        }

        try (var ps = session.createPreparedStatement(sql, variable)) {
            tm.execute(transaction -> {
                TgParameterList param;
                switch (0) {
                default:
                    // TgParameterList.of()を使う場合、値のデータ型はvariableで指定されたデータ型と一致していなければならない
                    param = TgParameterList.of().add("foo", 123).add("bar", 456L).add("zzz", "abc");
                    break;
                case 1:
                    // TgParameterList.of(variable)を使う場合、値はvariableで指定されたデータ型に変換される
                    param = TgParameterList.of(variable).add("foo", 123).add("bar", 456).add("zzz", "abc");
                    break;
                }

                try (var result = ps.execute(transaction, param)) {
                    System.out.println(result.getUpdateCount());
                }
            });
        }
    }

    void insertEntity(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));

        try (var ps = session.createPreparedStatement(TestEntity.INSERT_SQL, TestEntity.VARIABLE, TestEntity::toParameter)) {
            tm.execute(transaction -> {
//              var entity = new TestEntity(123, 456L, "abc");
                var entity = new TestEntity();
                entity.setFoo(123);
                entity.setBar(456L);
                entity.setZzz("abc");
                try (var result = ps.execute(transaction, entity)) {
                    System.out.println(result.getUpdateCount());
                }
            });
        }
    }

    void insertEntityMapping(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));

        var sql = "insert into TEST values(:foo, :bar, :zzz)";

        TgParameterMapping<TestEntity> parameterMapping;
        switch (0) {
        default:
            parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .int4("foo", TestEntity::getFoo) //
                    .int8("bar", TestEntity::getBar) //
                    .character("zzz", TestEntity::getZzz);
            break;
        case 1:
            parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .add("foo", TgDataType.INT4, TestEntity::getFoo) //
                    .add("bar", TgDataType.INT8, TestEntity::getBar) //
                    .add("zzz", TgDataType.CHARACTER, TestEntity::getZzz);
            break;
        case 2:
            parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .add("foo", int.class, TestEntity::getFoo) //
                    .add("bar", long.class, TestEntity::getBar) //
                    .add("zzz", String.class, TestEntity::getZzz);
            break;
        }

        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var entity = new TestEntity(123, 456L, "abc");
                try (var result = ps.execute(transaction, entity)) {
                    System.out.println(result.getUpdateCount());
                }
            });
        }
    }

    void insertForkJoin(TsurugiSession session, List<TestEntity> entityList) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));

        try (var ps = session.createPreparedStatement(TestEntity.INSERT_SQL, TestEntity.VARIABLE, TestEntity::toParameter)) {
            tm.execute(transaction -> {
                var task = new InsertTask(transaction, ps, entityList).fork();
                task.join();
            });
        }
    }

    @SuppressWarnings("serial")
    static class InsertTask extends RecursiveAction {
        private final TsurugiTransaction transaction;
        private final TsurugiPreparedStatementUpdate1<TestEntity> preparedStatement;
        private final List<TestEntity> list;

        public InsertTask(TsurugiTransaction transaction, TsurugiPreparedStatementUpdate1<TestEntity> preparedStatement, List<TestEntity> list) {
            this.transaction = transaction;
            this.preparedStatement = preparedStatement;
            this.list = list;
        }

        @Override
        protected void compute() {
            if (list.size() <= 100) {
                for (TestEntity entity : list) {
                    try {
                        try (var result = preparedStatement.execute(transaction, entity)) {
                            // System.out.println(result.getUpdateCount());
                        }
                    } catch (TsurugiTransactionIOException e) {
                        throw new TsurugiTransactionUncheckedIOException(e);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            } else {
                int n = list.size() / 2;
                var task1 = new InsertTask(transaction, preparedStatement, list.subList(0, n)).fork();
                var task2 = new InsertTask(transaction, preparedStatement, list.subList(n, list.size())).fork();
                task1.join();
                task2.join();
            }
        }
    }
}
