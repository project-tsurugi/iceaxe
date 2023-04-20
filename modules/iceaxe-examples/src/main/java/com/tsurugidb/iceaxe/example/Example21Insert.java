package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.RecursiveAction;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * insert example
 */
public class Example21Insert {

    void main() throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofLTX("TEST"));
            var tm = session.createTransactionManager(setting);

            insert_executeStatement(session, tm);
            insert_executeAndGetCount(session, tm);
            insert_tm(session, tm);
            insert_tm_sql(tm);

            insertBindParameter(session, tm);
            insertEntity(session, tm);
            insertEntityMapping(session, tm);
            insertForkJoin(session, tm, List.of(/* entities */));
        }
    }

    void insert_executeStatement(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createStatement("insert into TEST values(123, 456, 'abc')")) {
            int count = tm.execute(transaction -> {
                try (var result = transaction.executeStatement(ps)) {
                    return result.getUpdateCount();
                }
            });
            System.out.println(count);
        }
    }

    void insert_executeAndGetCount(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createStatement("insert into TEST values(123, 456, 'abc')")) {
            int count = tm.execute(transaction -> {
                return transaction.executeAndGetCount(ps);
            });
            System.out.println(count);
        }
    }

    void insert_tm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createStatement("insert into TEST values(123, 456, 'abc')")) {
            int count = tm.executeAndGetCount(ps);
            System.out.println(count);
        }
    }

    void insert_tm_sql(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        int count = tm.executeAndGetCount("insert into TEST values(123, 456, 'abc')");
        System.out.println(count);
    }

    void insertBindParameter(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var zzz = TgBindVariable.ofString("zzz");

        String sql;
        TgBindVariables variables;
        switch (0) {
        default:
//          sql = "insert into TEST values(" + foo + ", " + bar + ", " + zzz + ")";
            variables = TgBindVariables.of(foo, bar, zzz);
            sql = "insert into TEST values(" + variables.getSqlNames() + ")";
            break;
        case 1:
            sql = "insert into TEST values(:foo, :bar, :zzz)";
            variables = TgBindVariables.of().addInt("foo").addLong("bar").addString("zzz");
            break;
        case 2:
            sql = "insert into TEST values(:foo, :bar, :zzz)";
            variables = TgBindVariables.of().add("foo", TgDataType.INT).add("bar", TgDataType.LONG).add("zzz", TgDataType.STRING);
            break;
        case 3:
            sql = "insert into TEST values(:foo, :bar, :zzz)";
            variables = TgBindVariables.of().add("foo", int.class).add("bar", long.class).add("zzz", String.class);
            break;
        }
        var parameterMapping = TgParameterMapping.of(variables);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                TgBindParameters parameter;
                switch (0) {
                default:
                    parameter = TgBindParameters.of(foo.bind(123), bar.bind(456), zzz.bind("abc"));
                    break;
                case 1:
                    // TgBindParameters.of().add()を使う場合、値のデータ型はvariableで指定されたデータ型と一致していなければならない
                    parameter = TgBindParameters.of().add("foo", 123).add("bar", 456L).add("zzz", "abc");
                    break;
                case 2:
                    parameter = TgBindParameters.of().addInt("foo", 123).addLong("bar", 456).addString("zzz", "abc");
                    break;
                }

                int count = transaction.executeAndGetCount(ps, parameter);
                System.out.println(count);
            });
        }
    }

    void insertEntity(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createStatement(TestEntity.INSERT_SQL, TgParameterMapping.of(TestEntity.VARIABLES, TestEntity::toParameter))) {
            tm.execute(transaction -> {
//              var entity = new TestEntity(123, 456L, "abc");
                var entity = new TestEntity();
                entity.setFoo(123);
                entity.setBar(456L);
                entity.setZzz("abc");

                int count = transaction.executeAndGetCount(ps, entity);
                System.out.println(count);
            });
        }
    }

    void insertEntityMapping(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "insert into TEST values(:foo, :bar, :zzz)";

        TgParameterMapping<TestEntity> parameterMapping;
        switch (0) {
        default:
            parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("foo", TestEntity::getFoo) //
                    .addLong("bar", TestEntity::getBar) //
                    .addString("zzz", TestEntity::getZzz);
            break;
        case 1:
            parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .add("foo", TgDataType.INT, TestEntity::getFoo) //
                    .add("bar", TgDataType.LONG, TestEntity::getBar) //
                    .add("zzz", TgDataType.STRING, TestEntity::getZzz);
            break;
        case 2:
            parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .add("foo", int.class, TestEntity::getFoo) //
                    .add("bar", long.class, TestEntity::getBar) //
                    .add("zzz", String.class, TestEntity::getZzz);
            break;
        }

        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var entity = new TestEntity(123, 456L, "abc");
                int count = transaction.executeAndGetCount(ps, entity);
                System.out.println(count);
            });
        }
    }

    void insertForkJoin(TsurugiSession session, TsurugiTransactionManager tm, List<TestEntity> entityList) throws IOException, InterruptedException {
        try (var ps = session.createStatement(TestEntity.INSERT_SQL, TgParameterMapping.of(TestEntity.VARIABLES, TestEntity::toParameter))) {
            tm.execute(transaction -> {
                var task = new InsertTask(transaction, ps, entityList).fork();
                task.join();
            });
        }
    }

    @SuppressWarnings("serial")
    static class InsertTask extends RecursiveAction {
        private final TsurugiTransaction transaction;
        private final TsurugiSqlPreparedStatement<TestEntity> preparedStatement;
        private final List<TestEntity> entityList;

        public InsertTask(TsurugiTransaction transaction, TsurugiSqlPreparedStatement<TestEntity> preparedStatement, List<TestEntity> entityList) {
            this.transaction = transaction;
            this.preparedStatement = preparedStatement;
            this.entityList = entityList;
        }

        @Override
        protected void compute() {
            if (entityList.size() <= 100) {
                for (TestEntity entity : entityList) {
                    try {
                        transaction.executeAndGetCount(preparedStatement, entity);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (TsurugiTransactionException e) {
                        throw new TsurugiTransactionRuntimeException(e);
                    }
                }
            } else {
                int n = entityList.size() / 2;
                var task1 = new InsertTask(transaction, preparedStatement, entityList.subList(0, n)).fork();
                var task2 = new InsertTask(transaction, preparedStatement, entityList.subList(n, entityList.size())).fork();
                // FIXME TsurugiTransactionException例外処理
                task1.join();
                task2.join();
            }
        }
    }
}
