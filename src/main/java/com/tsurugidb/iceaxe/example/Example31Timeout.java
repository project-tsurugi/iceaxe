package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgTxOptionList;

/**
 * specify timeout example
 */
public class Example31Timeout {

    private static final TgTxOptionList TX_OPTION = TgTxOptionList.of(TgTxOption.ofOCC());

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("dbname");
        timeout1(connector);
        timeout2(connector);
        timeout3(connector);
    }

    void timeout1(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        info.timeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager(TX_OPTION);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeout2(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager(TX_OPTION);
            tm.setCommitTimeout(1, TimeUnit.HOURS);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeout3(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager(TX_OPTION);
            tm.execute(transaction -> {
                transaction.setCommitTimeout(1, TimeUnit.HOURS);

                // do sql
            });
        }
    }
}
