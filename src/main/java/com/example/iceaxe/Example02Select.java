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
            select(session);
            selectAsList(session);
            selectUserEntity(session);
            selectUserEntityList(session);
            selectUserEntityListMapping(session);

            selectByParameter1(session);
            selectByParameter2(session);
            selectByParameter2Bind(session);
            selectByParameter2AsUserEntityList(session);
        }
    }

    void select(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST")) {
            tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    List<String> nameList = result.getNameList();
                    System.out.println(nameList);

                    for (TsurugiResultEntity record : result) {
                        // FIXME カラム名は大文字にすべき？
                        System.out.println(record.getInt4OrNull("foo"));
//TODO                  System.out.println(record.getInt8OrNull("bar"));
//TODO                  System.out.println(record.getCharacterOrNull("zzz"));
                    }
                }
            });
        }
    }

    void selectAsList(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST")) {
            List<TsurugiResultEntity> list = tm.execute(transaction -> {
                // FIXME ps.executeAndGetListまたはps.getRecordListみたいなメソッドがあると便利？あるいは戻り値収集部分をラムダ式で渡すとか？
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectUserEntity(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST", TgResultMapping.of(TestEntity::of))) {
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

    void selectUserEntityList(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        try (var ps = session.createPreparedQuery("select * from TEST", TgResultMapping.of(TestEntity::of))) {
            List<TestEntity> list = tm.execute(transaction -> {
                try (var result = ps.execute(transaction)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }

    void selectUserEntityListMapping(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        var sql = "select FOO, BAR, ZZZ from TEST";
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .int4(TestEntity::setFoo) //
//TODO                .int8(TestEntity::setBar) //
//TODO                .character(TestEntity::setZzz)
        ;

        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
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

    void selectByParameter2AsUserEntityList(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_ONLY));

        var sql = "select * from TEST where FOO = :foo and BAR <= :bar";
        var variable = TgVariableList.of().int4("foo").int8("bar");
        var parameterMapping = TgParameterMapping.of(variable);
        var resultMapping = TgResultMapping.of(TestEntity::of);
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            List<TestEntity> list = tm.execute(transaction -> {
                var param = TgParameterList.of().add("foo", 123).add("bar", 456L);
                try (var result = ps.execute(transaction, param)) {
                    return result.getRecordList();
                }
            });
            System.out.println(list);
        }
    }
}
