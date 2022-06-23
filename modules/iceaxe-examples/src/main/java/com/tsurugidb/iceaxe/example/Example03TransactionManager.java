package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

/**
 * TsurugiTransactionManager example
 * 
 * <p>
 * TransactionManagerのexecuteメソッドは、ひとつのトランザクションを実行する。<br>
 * 実行が正常に終了した場合はコミットし、例外が発生した場合はロールバックする。<br>
 * トランザクション内でリトライ可能なアボートが発生した場合は、自動的にトランザクションの先頭から再実行を行う。
 * </p>
 * <p>
 * トランザクションは、指定されたトランザクションモードで実行する。<br>
 * TsurugiTransactionOptionListでは、1回目の実行は１番目の要素、2回目の実行には2番目の要素のトランザクションモードが使用される。<br>
 * TsurugiTransactionOptionListで指定された要素数以上の回数アボートした場合は、executeメソッドから例外がスローされる。
 * </p>
 */
public class Example03TransactionManager {

    private static final TgTxOption OCC = TgTxOption.ofOCC();
    private static final TgTxOption LTX = TgTxOption.ofLTX("table1", "table2");

    /**
     * @see Example02Session
     */
    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            manager1(session);
            manager2(session);
            managerAlways(session);
        }
    }

    void manager1(TsurugiSession session) throws IOException {
        var setting = TgTmSetting.of(OCC, LTX);
        var tm = session.createTransactionManager(setting);

        tm.execute(transaction -> {
//          preparedStatement.execute(transaction)
        });
    }

    void manager2(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager();

        var setting = TgTmSetting.of(OCC, LTX);
        tm.execute(setting, transaction -> {
//          preparedStatement.execute(transaction)
        });
    }

    void managerAlways(TsurugiSession session) throws IOException {
        var setting = TgTmSetting.ofAlways(OCC);
        var tm = session.createTransactionManager(setting);

        tm.execute(transaction -> {
//          preparedStatement.execute(transaction)
        });
    }
}
