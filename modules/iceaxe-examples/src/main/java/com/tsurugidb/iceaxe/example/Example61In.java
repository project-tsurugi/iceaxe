package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariableList;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;

/**
 * 'in' example
 */
public class Example61In {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
            var tm = session.createTransactionManager(setting);

            in1(session, tm, List.of(/* foo list */));
        }
    }

    void in1(TsurugiSession session, TsurugiTransactionManager tm, List<Integer> fooList) throws IOException {
        var vlist = TgVariableList.of();
        var plist = TgParameterList.of();
        int i = 0;
        for (var foo : fooList) {
            var variable = TgVariable.ofInt4("f" + (i++));
            vlist.add(variable);
            plist.add(variable.bind(foo));
        }

        var sql = "select * from TEST where foo in(" + vlist.getSqlNames() + ")";
        var parameterMapping = TgParameterMapping.of(vlist);
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .int4(TestEntity::setFoo) //
                .int8(TestEntity::setBar) //
                .character(TestEntity::setZzz);
        try (var ps = session.createPreparedQuery(sql, parameterMapping, resultMapping)) {
            var list = ps.executeAndGetList(tm, plist);
            for (var entity : list) {
                System.out.println(entity);
            }
        }
    }
}
