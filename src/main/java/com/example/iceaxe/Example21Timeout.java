package com.example.iceaxe;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugi.iceaxe.TsurugiConnector;
import com.tsurugi.iceaxe.session.TgSessionInfo;
import com.tsurugi.iceaxe.session.TgSessionInfo.TgTimeoutKey;

/**
 * specify timeout example
 */
public class Example21Timeout {

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
            var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeout2(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));
            tm.setCommitTimeout(1, TimeUnit.HOURS);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeout3(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager(List.of(TransactionOptionExample.OCC));
            tm.execute(transaction -> {
                transaction.setCommitTimeout(1, TimeUnit.HOURS);

                // do sql
            });
        }
    }
}
