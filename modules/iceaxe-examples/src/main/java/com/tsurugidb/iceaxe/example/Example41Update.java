package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * update example
 */
public class Example41Update {

    void main() throws IOException {
        try (var session = Example02Session.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofLTX("TEST"));
            var tm = session.createTransactionManager(setting);

            update0(session, tm);
            updateParameter(session, tm);
            updateEntity(session, tm);
        }
    }

    void update0(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        try (var ps = session.createStatement("update TEST set bar = 0")) {
            int count = tm.executeAndGetCount(ps);
            System.out.println(count);
        }
    }

    void update0Direct(TsurugiTransactionManager tm) throws IOException {
        int count = tm.executeAndGetCount("update TEST set bar = 0");
        System.out.println(count);
    }

    void updateParameter(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgBindVariable.ofInt("foo");
        var add = TgBindVariable.ofLong("add");

        var sql = "update TEST set bar = bar + :add where foo = :foo";
        var vlist = TgBindVariables.of(foo, add);

        try (var ps = session.createStatement(sql, TgParameterMapping.of(vlist))) {
            tm.execute(transaction -> {
                var plist = TgBindParameters.of(foo.bind(123), add.bind(1));
                int count = transaction.executeAndGetCount(ps, plist);
                System.out.println(count);
            });
        }
    }

    void updateEntity(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var sql = "update TEST set" //
                + " bar = :bar" //
                + " zzz = :zzz" //
                + " where foo = :foo";
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
}
