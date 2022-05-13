package com.example.iceaxe;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import com.tsurugi.iceaxe.TsurugiConnector;
import com.tsurugi.iceaxe.result.TgResultMapping;
import com.tsurugi.iceaxe.result.TsurugiResultEntity;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.statement.TgParameterList;
import com.tsurugi.iceaxe.statement.TgParameterMapping;
import com.tsurugi.iceaxe.statement.TgVariable;
import com.tsurugi.iceaxe.statement.TgVariableList;

/**
 * select example
 */
public class Example02Select {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("dbname");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            selectLoop(session);

            selectAsList_execRs(session);
            selectAsList_execPs(session);
            selectAsList_execTm(session);

            selectAsUserEntityLoop(session);
            selectAsUserEntityList(session);

            selectByParameter1(session);
            selectByParameter2(session);
            selectByParameter2Bind(session);
            selectByParameter2AsUserEntityList_execRs(session);
            selectByParameter2AsUserEntityList_execPs(session);
            selectByParameter2AsUserEntityList_execTm(session);
        }
    }

    void selectLoop(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST")) {
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    List<String> nameList = result.getNameList();
                    System.out.println(nameList);

                    for (TsurugiResultEntity record : result) {
                        System.out.println(record.getInt4OrNull("foo"));
//TODO                  System.out.println(record.getInt8OrNull("bar"));
//TODO                  System.out.println(record.getCharacterOrNull("zzz"));
                    }
                }
            });
        }
    }

    void selectAsList_execRs(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectAsList_execPs(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                return ps.executeAndGetList(transaction);
            });
            System.out.println(list);
        }
    }

    void selectAsList_execTm(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = ps.executeAndGetList(tm);
            System.out.println(list);
        }
    }

    void selectAsUserEntityLoop(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

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
//TODO          entity.setBar(record.getInt8OrNull("bar"));
//TODO          entity.setZzz(record.getCharacterOrNull("zzz"));
                return entity;
            });
        case 1:
            return TgResultMapping.of(TestEntity::of);
        case 2:
            return TgResultMapping.of(TestEntity::new) //
                    .int4(TestEntity::setFoo) //
//TODO              .int8(TestEntity::setBar) //
//TODO              .character(TestEntity::setZzz)
            ;
        }
    }

    void selectAsUserEntityList(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST", resultMappingForTestEntity())) {
            List<TestEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter1(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

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

    void selectByParameter2(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

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

    void selectByParameter2Bind(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var variable = TgVariableList.of(foo, bar);
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(variable))) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of(foo.bind(123), bar.bind(456L));
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execRs(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var variable = TgVariableList.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of(foo.bind(123), bar.bind(456L));
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execPs(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var variable = TgVariableList.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of(foo.bind(123), bar.bind(456L));
                return ps.executeAndGetList(transaction, param);
            });
            System.out.println(list);
        }
    }

    void selectByParameter2AsUserEntityList_execTm(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var foo = TgVariable.ofInt4("foo");
        var bar = TgVariable.ofInt8("bar");
        var variable = TgVariableList.of(foo, bar);
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = resultMappingForTestEntity();
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            var param = TgParameterList.of(foo.bind(123), bar.bind(456L));
            List<TestEntity> list = ps.executeAndGetList(tm, param);
            System.out.println(list);
        }
    }
}
