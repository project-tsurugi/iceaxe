package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * select example
 */
public class Example31Select {

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            Example11Ddl.dropAndCreateTable(session);
            Example21Insert.insert(session);

            new Example31Select().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
        var tm = session.createTransactionManager(setting);

        selectLoop(session, tm);

        selectAsList_executeQuery(session, tm);
        selectAsList_executeAndGetList(session, tm);
        selectAsList_tm(session, tm);
        selectAsList_tm_sql(tm);

        selectAsEntityLoop(session, tm);
        selectAsEntityList(session, tm);

        selectByParameter1(session, tm);
        selectByParameter1_singleVariable(session, tm);
        selectByParameter2(session, tm);
        selectByParameter2_bindVariable(session, tm);
        selectByParameter2_asEntityList_executeQuery(session, tm);
        selectByParameter2_asEntityList_executeAndGetList(session, tm);
        selectByParameter2_asEntityList_tm(session, tm);
        selectByParameter2_asEntityList_tm_sql(tm);
    }

    void selectLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select * from TEST")) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    List<String> nameList = result.getNameList();
                    System.out.println("nameList=" + nameList);

                    for (TsurugiResultEntity entity : result) {
                        System.out.print("FOO=" + entity.getIntOrNull("FOO"));
                        System.out.print(", BAR=" + entity.getLongOrNull("BAR"));
                        System.out.println(", ZZZ=" + entity.getStringOrNull("ZZZ"));
                    }
                }
            });
        }
    }

    void selectAsList_executeQuery(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectAsList_executeAndGetList(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
            });
            System.out.println(list);
        }
    }

    void selectAsList_tm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            System.out.println(list);
        }
    }

    void selectAsList_tm_sql(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        List<TsurugiResultEntity> list = tm.executeAndGetList("select * from TEST");
        System.out.println(list);
    }

    void selectAsEntityLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery("select * from TEST", resultMapping)) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    for (TestEntity entity : result) {
                        System.out.print("FOO=" + entity.getFoo());
                        System.out.print(", BAR=" + entity.getBar());
                        System.out.println(", ZZZ=" + entity.getZzz());
                    }
                }
            });
        }
    }

    private TgResultMapping<TestEntity> resultMappingForTestEntity() {
        switch (0) {
        default:
            return TgResultMapping.of(TestEntity::new) //
                    .addInt("FOO", TestEntity::setFoo) //
                    .addLong("BAR", TestEntity::setBar) //
                    .addString("ZZZ", TestEntity::setZzz);
        case 1:
            return TgResultMapping.of(TestEntity::of);
        case 2: // selectのカラムの順序依存
            return TgResultMapping.of(TestEntity::new) //
                    .addInt(TestEntity::setFoo) //
                    .addLong(TestEntity::setBar) //
                    .addString(TestEntity::setZzz);
        case 3: // selectのカラムの順序依存
            return TgResultMapping.of(TestEntity::new) //
                    .addInt(0, TestEntity::setFoo) //
                    .addLong(1, TestEntity::setBar) //
                    .addString(2, TestEntity::setZzz);
        case 10:
            return TgResultMapping.of(record -> {
                var entity = new TestEntity();
                entity.setFoo(record.getIntOrNull("FOO"));
                entity.setBar(record.getLongOrNull("BAR"));
                entity.setZzz(record.getStringOrNull("ZZZ"));
                return entity;
            });
        case 12: // selectのカラムの順序依存
            return TgResultMapping.of(record -> {
                var entity = new TestEntity();
                entity.setFoo(record.nextIntOrNull());
                entity.setBar(record.nextLongOrNull());
                entity.setZzz(record.nextStringOrNull());
                return entity;
            });
        case 13: // selectのカラムの順序依存
            return TgResultMapping.of(record -> {
                var entity = new TestEntity();
                entity.setFoo(record.getIntOrNull(0));
                entity.setBar(record.getLongOrNull(1));
                entity.setZzz(record.getStringOrNull(2));
                return entity;
            });
        }
    }

    void selectAsEntityList(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery("select * from TEST", resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
            });
            System.out.println(list);
        }
    }

    void selectByParameter1(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select * from TEST where FOO = :foo";
        var variables = TgBindVariables.of().addInt("foo");
        Function<Integer, TgBindParameters> parameterConverter = foo -> TgBindParameters.of().add("foo", foo);
        var parameterMapping = TgParameterMapping.of(variables, parameterConverter);
        try (var ps = session.createQuery(sql, parameterMapping)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                int parameter = 123;
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter1_singleVariable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select * from TEST where FOO = :foo";
        var parameterMapping = TgParameterMapping.ofSingle("foo", int.class);
        try (var ps = session.createQuery(sql, parameterMapping)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                int parameter = 123;
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var variables = TgBindVariables.of().addInt("foo").addLong("bar");
        var parameterMapping = TgParameterMapping.of(variables);
        try (var ps = session.createQuery(sql, parameterMapping)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of().add("foo", 123).add("bar", 456L);
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2_bindVariable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
//      var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var parameterMapping = TgParameterMapping.of(foo, bar);
        try (var ps = session.createQuery(sql, parameterMapping)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2_asEntityList_executeQuery(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var parameterMapping = TgParameterMapping.of(foo, bar);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
                try (var rs = transaction.executeQuery(ps, parameter)) {
                    return rs.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2_asEntityList_executeAndGetList(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var parameterMapping = TgParameterMapping.of(foo, bar);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2_asEntityList_tm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var parameterMapping = TgParameterMapping.of(foo, bar);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
            List<TestEntity> list = tm.executeAndGetList(ps, parameter);
            System.out.println(list);
        }
    }

    void selectByParameter2_asEntityList_tm_sql(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var parameterMapping = TgParameterMapping.of(foo, bar);
        var resultMapping = resultMappingForTestEntity();
        var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
        List<TestEntity> list = tm.executeAndGetList(sql, parameterMapping, parameter, resultMapping);
        System.out.println(list);
    }
}
