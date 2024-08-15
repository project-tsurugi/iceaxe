package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultCount;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * update example
 */
public class Example41Update {

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            Example11Ddl.dropAndCreateTable(session);
            Example21Insert.insert(session);

            new Example41Update().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofLTX("TEST"));
        var tm = session.createTransactionManager(setting);

        update_tm(session, tm);
        update_tm_sql(tm);
        updateBindParameter(session, tm);
        updateEntity(session, tm);
        update_countDetail(session, tm);
    }

    void update_tm(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        try (var ps = session.createStatement("update TEST set BAR = 0")) {
            int count = tm.executeAndGetCount(ps);
            System.out.println(count);
        }
    }

    void update_tm_sql(TsurugiTransactionManager tm) throws IOException, InterruptedException {
        int count = tm.executeAndGetCount("update TEST set BAR = 0");
        System.out.println(count);
    }

    void updateBindParameter(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var foo = TgBindVariable.ofInt("foo");
        var add = TgBindVariable.ofLong("add");

        var sql = "update TEST set BAR = BAR + :add where FOO = :foo";
        var parameterMapping = TgParameterMapping.of(foo, add);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(foo.bind(123), add.bind(1));
                int count = transaction.executeAndGetCount(ps, parameter);
                System.out.println(count);
            });
        }
    }

    void updateEntity(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "update TEST set" //
                + " BAR = :bar," //
                + " ZZZ = :zzz" //
                + " where FOO = :foo";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);

        try (var ps = session.createStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var entity = new TestEntity(123, 456L, "abc");
                int count = transaction.executeAndGetCount(ps, entity);
                System.out.println(count);
            });
        }
    }

    void update_countDetail(TsurugiSession session, TsurugiTransactionManager tm) throws IOException, InterruptedException {
        var sql = "update TEST set BAR = BAR + 1";
        try (var ps = session.createStatement(sql)) {
            tm.execute(transaction -> {
                TgResultCount count = transaction.executeAndGetCountDetail(ps);
                long updatedCount = count.getUpdatedCount();
                System.out.println(updatedCount);
            });
        }
    }
}
