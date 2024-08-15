package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 'in' example
 */
public class Example71In {

    public static void main(String... args) throws IOException, InterruptedException {
        try (var session = Example02Session.createSession()) {
            Example11Ddl.dropAndCreateTable(session);
            Example21Insert.insert(session);

            new Example71In().main(session);
        }
    }

    void main(TsurugiSession session) throws IOException, InterruptedException {
        var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
        var tm = session.createTransactionManager(setting);

        in1(session, tm, List.of(122, 124));
    }

    void in1(TsurugiSession session, TsurugiTransactionManager tm, List<Integer> fooList) throws IOException, InterruptedException {
        var variables = TgBindVariables.of();
        var parameter = TgBindParameters.of();
        int i = 0;
        for (var foo : fooList) {
            var variable = TgBindVariable.ofInt("f" + (i++));
            variables.add(variable);
            parameter.add(variable.bind(foo));
        }

        var sql = "select * from TEST where FOO in(" + variables.getSqlNames() + ")";
        System.out.println(sql);
        var parameterMapping = TgParameterMapping.of(variables);
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt(TestEntity::setFoo) //
                .addLong(TestEntity::setBar) //
                .addString(TestEntity::setZzz);
        try (var ps = session.createQuery(sql, parameterMapping, resultMapping)) {
            var list = tm.executeAndGetList(ps, parameter);
            for (var entity : list) {
                System.out.println(entity);
            }
        }
    }
}
