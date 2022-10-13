package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

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
        try (var ps = session.createPreparedStatement("update TEST set bar = 0")) {
            int count = ps.executeAndGetCount(tm);
            System.out.println(count);
        }
    }

    void updateParameter(TsurugiSession session, TsurugiTransactionManager tm) throws IOException {
        var foo = TgVariable.ofInt4("foo");
        var add = TgVariable.ofInt8("add");

        var sql = "update TEST set bar = bar + :add where foo = :foo";
        var vlist = TgVariableList.of(foo, add);

        try (var ps = session.createPreparedStatement(sql, TgParameterMapping.of(vlist))) {
            tm.execute(transaction -> {
                var plist = TgParameterList.of(foo.bind(123), add.bind(1));
                int count = ps.executeAndGetCount(transaction, plist);
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
                .int4("foo", TestEntity::getFoo) //
                .int8("bar", TestEntity::getBar) //
                .character("zzz", TestEntity::getZzz);

        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            tm.execute(transaction -> {
                var entity = new TestEntity(123, 456L, "abc");
                int count = ps.executeAndGetCount(transaction, entity);
                System.out.println(count);
            });
        }
    }
}
