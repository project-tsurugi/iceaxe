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

    void main() throws IOException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
            var tm = session.createTransactionManager(setting);

            selectLoop(session, tm);

            selectAsList_execRs(session, tm);
            selectAsList_execPs(session, tm);
            selectAsList_execTm(session, tm);

            selectAsEntityLoop(session, tm);
            select_asEntityList(session, tm);

            selectByParameter1(session, tm);
            selectByParameter1_singleVariable(session, tm);
            selectByParameter2(session, tm);
            selectByParameter2_bindVariable(session, tm);
            selectByParameter2_asEntityList_execRs(session, tm);
            selectByParameter2_asEntityList_execPs(session, tm);
            selectByParameter2_asEntityList_execTm(session, tm);
        }
    }

    void selectLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST")) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    List<String> nameList = result.getNameList();
                    System.out.println(nameList);

                    for (TsurugiResultEntity entity : result) {
                        System.out.println(entity.getIntOrNull("foo"));
                        System.out.println(entity.getLongOrNull("bar"));
                        System.out.println(entity.getStringOrNull("zzz"));
                    }
                }
            });
        }
    }

    void selectAsList_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectAsList_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
            });
            System.out.println(list);
        }
    }

    void selectAsList_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            System.out.println(list);
        }
    }

    void selectAsList_execTmDirect(TsurugiTransactionManager tm) throws IOException {
        List<TsurugiResultEntity> list = tm.executeAndGetList("select * from TEST");
        System.out.println(list);
    }

    void selectAsEntityLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST", resultMappingForTestEntity())) {
            tm.execute(transaction -> {
                try (var result = transaction.executeQuery(ps)) {
                    for (TestEntity entity : result) {
                        System.out.println(entity.getFoo());
                        System.out.println(entity.getBar());
                        System.out.println(entity.getZzz());
                    }
                }
            });
        }
    }

    private TgResultMapping<TestEntity> resultMappingForTestEntity() {
        switch (4) {
        default:
            return TgResultMapping.of(record -> {
                var entity = new TestEntity();
                entity.setFoo(record.getIntOrNull("foo"));
                entity.setBar(record.getLongOrNull("bar"));
                entity.setZzz(record.getStringOrNull("zzz"));
                return entity;
            });
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
        case 4:
            return TgResultMapping.of(TestEntity::new) //
                    .addInt("foo", TestEntity::setFoo) //
                    .addLong("bar", TestEntity::setBar) //
                    .addString("zzz", TestEntity::setZzz);
        }
    }

    void select_asEntityList(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST", resultMappingForTestEntity())) {
            List<TestEntity> list = tm.execute(transaction -> {
                return transaction.executeAndGetList(ps);
            });
            System.out.println(list);
        }
    }

    void selectByParameter1(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

    void selectByParameter1_singleVariable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from TEST where FOO = :foo";
        var parameterMapping = TgParameterMapping.of("foo", int.class);
        try (var ps = session.createQuery(sql, parameterMapping)) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                int parameter = 123;
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

    void selectByParameter2_bindVariable(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

    void selectByParameter2_asEntityList_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

    void selectByParameter2_asEntityList_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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

    void selectByParameter2_asEntityList_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
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
}
