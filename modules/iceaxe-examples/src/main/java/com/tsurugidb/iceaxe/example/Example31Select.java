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

            selectAsUserEntityLoop(session, tm);
            selectAsUserEntityList(session, tm);

            selectByParameter1(session, tm);
            selectByParameter2(session, tm);
            selectByParameter2Bind(session, tm);
            selectByParameter2AsUserEntityList_execRs(session, tm);
            selectByParameter2AsUserEntityList_execPs(session, tm);
            selectByParameter2AsUserEntityList_execTm(session, tm);
        }
    }

    void selectLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST")) {
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    List<String> nameList = result.getNameList();
                    System.out.println(nameList);

                    for (TsurugiResultEntity record : result) {
                        System.out.println(record.getIntOrNull("foo"));
                        System.out.println(record.getLongOrNull("bar"));
                        System.out.println(record.getStringOrNull("zzz"));
                    }
                }
            });
        }
    }

    void selectAsList_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
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

    void selectAsUserEntityLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST", resultMappingForTestEntity())) {
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
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
        switch (0) {
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

    void selectAsUserEntityList(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createQuery("select * from TEST", resultMappingForTestEntity())) {
            List<TestEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter1(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from TEST where FOO = :foo";
        var variable = TgBindVariables.of().addInt("foo");
        Function<Integer, TgBindParameters> parameterConverter = foo -> TgBindParameters.of().add("foo", foo);
        try (var ps = session.createQuery(sql, TgParameterMapping.of(variable, parameterConverter))) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                int param = 123;
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var variable = TgBindVariables.of().addInt("foo").addLong("bar");
        try (var ps = session.createQuery(sql, TgParameterMapping.of(variable))) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of().add("foo", 123).add("bar", 456L);
                try (var result = ps.execute(transaction, parameter)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2Bind(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
//      var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgBindVariables.of(foo, bar);
        try (var ps = session.createQuery(sql, TgParameterMapping.of(variable))) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
                try (var result = ps.execute(transaction, parameter)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgBindVariables.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
                try (var result = ps.execute(transaction, parameter)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgBindVariables.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
                return transaction.executeAndGetList(ps, parameter);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgBindVariables.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            var parameter = TgBindParameters.of(foo.bind(123), bar.bind(456));
            List<TestEntity> list = tm.executeAndGetList(ps, parameter);
            System.out.println(list);
        }
    }
}
