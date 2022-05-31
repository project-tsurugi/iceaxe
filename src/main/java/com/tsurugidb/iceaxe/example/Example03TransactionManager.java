package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * TsurugiTransactionManager example
 * 
 * <p>
 * TransactionManagerのexecuteメソッドは、ひとつのトランザクションを実行する。<br>
 * 実行が正常に終了した場合はコミットし、例外が発生した場合はロールバックする。<br>
 * トランザクション内でリトライ可能なアボートが発生した場合は、自動的にトランザクション全体の再実行を行う。
 * </p>
 * <p>
 * トランザクションを実行する際には、指定されたトランザクションモードで実行する。<br>
 * トランザクションモードはListで指定し、1回目の実行は１番目の要素、2回目の実行には2番目の要素のトランザクションモードが使用される。<br>
 * Listで指定された要素数以上にアボートした場合は、executeメソッドから例外がスローされる。
 * </p>
 */
public class Example03TransactionManager {

    /**
     * @see Example02Session
     */
    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("dbname");
        try (var session = connector.createSession(TgSessionInfo.of("user", "password"))) {
            manager1(session);
            manager2(session);
        }
    }

    void manager1(TsurugiSession session) throws IOException {
        var optionList = List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_WRITE);
        var tm = session.createTransactionManager(optionList);

        tm.execute(transaction -> {
//          preparedStatement.execute(transaction)
        });
    }

    void manager2(TsurugiSession session) throws IOException {
        var tm = session.createTransactionManager();

        var optionList = List.of(TransactionOptionExample.OCC, TransactionOptionExample.BATCH_READ_WRITE);
        tm.execute(optionList, transaction -> {
//          preparedStatement.execute(transaction)
        });
    }
}
