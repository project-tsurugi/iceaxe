package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;

/**
 * select example
 */
public class Example12Select {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
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
        try (var ps = session.createPreparedQuery("select * from TEST")) {
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    List<String> nameList = result.getNameList();
                    System.out.println(nameList);

                    for (TsurugiResultEntity record : result) {
                        System.out.println(record.getInt4OrNull("foo"));
                        System.out.println(record.getInt8OrNull("bar"));
                        System.out.println(record.getCharacterOrNull("zzz"));
                    }
                }
            });
        }
    }

    void selectAsList_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectAsList_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                return ps.executeAndGetList(transaction);
            });
            System.out.println(list);
        }
    }

    void selectAsList_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = ps.executeAndGetList(tm);
            System.out.println(list);
        }
    }

    void selectAsUserEntityLoop(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select * from TEST", resultMappingForTestEntity())) {
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
                entity.setFoo(record.getInt4OrNull("foo"));
                entity.setBar(record.getInt8OrNull("bar"));
                entity.setZzz(record.getCharacterOrNull("zzz"));
                return entity;
            });
        case 1:
            return TgResultMapping.of(TestEntity::of);
        case 2: // selectのカラムの順序依存
            return TgResultMapping.of(TestEntity::new) //
                    .int4(TestEntity::setFoo) //
                    .int8(TestEntity::setBar) //
                    .character(TestEntity::setZzz);
        case 3: // selectのカラムの順序依存
            return TgResultMapping.of(TestEntity::new) //
                    .int4(0, TestEntity::setFoo) //
                    .int8(1, TestEntity::setBar) //
                    .character(2, TestEntity::setZzz);
        case 4:
            return TgResultMapping.of(TestEntity::new) //
                    .int4("foo", TestEntity::setFoo) //
                    .int8("bar", TestEntity::setBar) //
                    .character("zzz", TestEntity::setZzz);
        }
    }

    void selectAsUserEntityList(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createPreparedQuery("select * from TEST", resultMappingForTestEntity())) {
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
        var variable = TgVariableList.of().int4("foo");
        Function<Integer, TgParameterList> parameterConverter = foo -> TgParameterList.of().add("foo", foo);
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable, parameterConverter))) {
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
        var variable = TgVariableList.of().int4("foo").int8("bar");
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable))) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of().add("foo", 123).add("bar", 456L);
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2Bind(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
//      var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgVariableList.of(foo, bar);
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable))) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of(foo.bind(123), bar.bind(456));
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execRs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgVariableList.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of(foo.bind(123), bar.bind(456));
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execPs(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgVariableList.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of(foo.bind(123), bar.bind(456));
                return ps.executeAndGetList(transaction, param);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execTm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var sql = "select * from TEST where FOO = " + foo + " and BAR <= " + bar;
        var variable = TgVariableList.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            var param = TgParameterList.of(foo.bind(123), bar.bind(456));
            List<TestEntity> list = ps.executeAndGetList(tm, param);
            System.out.println(list);
        }
    }
}
