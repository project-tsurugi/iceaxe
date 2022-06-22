package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.RecursiveAction;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgTxOptionList;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionRuntimeException;

/**
 * insert example
 */
public class Example11Insert {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            var optionList = TgTxOptionList.of(TgTxOption.ofOCC(), TgTxOption.ofLTX("TEST"));
            var tm = session.createTransactionManager(optionList);

            insert0_execRs(session, tm);
            insert0_execPs(session, tm);
            insert0_execTm(session, tm);

            insertParameter(session, tm);
            insertEntity(session, tm);
            insertEntityMapping(session, tm);
            insertForkJoin(session, tm, List.of(/* entities */));
        }
    }

    void insert0_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedStatement("insert into TEST values(123, 456, 'abc')")) {
            int count = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getUpdateCount();
                }
            });
            System.out.println(count);
        }
    }

    void insert0_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedStatement("insert into TEST values(123, 456, 'abc')")) {
            int count = tm.execute(transaction -> {
                return ps.executeAndGetCount(transaction);
            });
            System.out.println(count);
        }
    }

    void insert0_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedStatement("insert into TEST values(123, 456, 'abc')")) {
            int count = ps.executeAndGetCount(tm);
            System.out.println(count);
        }
    }

    void insertParameter(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

        try (var ps = session.createPreparedStatement(sql, TgParameterMapping.of(variable))) {
            tm.execute(transaction -> {
                TgParameterList param;
                switch (0) {
                default:
                    // TgParameterList.of()を使う場合、値のデータ型はvariableで指定されたデータ型と一致していなければならない
                    param = TgParameterList.of().add("foo", 123).add("bar", 456L).add("zzz", "abc");
                    break;
                case 1:
                    param = TgParameterList.of().int4("foo", 123).int8("bar", 456).character("zzz", "abc");
                    break;
                case 2:
                    // TgParameterList.of(variable)を使う場合、値はvariableで指定されたデータ型に変換される
                    param = TgParameterList.of(variable).add("foo", 123).add("bar", 456).add("zzz", "abc");
                    break;
                case 3:
                    // TgParameterList.of(variable)でデータ型名のメソッドを使う場合、variableで指定されたデータ型と一致しない場合は実行時エラー
                    param = TgParameterList.of(variable).int4("foo", 123).int8("bar", 456).character("zzz", "abc");
                    break;
                }

                try (var result = ps.execute(transaction, param)) {
                    System.out.println(result.getUpdateCount());
                }
            });
        }
    }

    void insertEntity(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedStatement(TestEntity.INSERT_SQL, TgParameterMapping.of(TestEntity.VARIABLE, TestEntity::toParameter))) {
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

    void insertEntityMapping(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

    void insertForkJoin(TsurugiSession session, TsurugiTransactionManager tm, List<TestEntity> entityList) throws IOException {
        try (var ps = session.createPreparedStatement(TestEntity.INSERT_SQL, TgParameterMapping.of(TestEntity.VARIABLE, TestEntity::toParameter))) {
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
                    try (var result = preparedStatement.execute(transaction, entity)) {
                        // System.out.println(result.getUpdateCount());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (TsurugiTransactionException e) {
                        throw new TsurugiTransactionRuntimeException(e);
                    }
                }
            } else {
                int n = list.size() / 2;
                var task1 = new InsertTask(transaction, preparedStatement, list.subList(0, n)).fork();
                var task2 = new InsertTask(transaction, preparedStatement, list.subList(n, list.size())).fork();
                // FIXME TsurugiTransactionException例外処理
                task1.join();
                task2.join();
            }
        }
    }
}