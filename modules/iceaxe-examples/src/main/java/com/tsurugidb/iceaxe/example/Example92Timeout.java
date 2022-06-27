package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * example to specify timeout
 */
public class Example92Timeout {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        timeout1(connector);
        timeout2(connector);
        timeout3(connector);
    }

    void timeout1(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        info.timeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeout2(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitTimeout(1, TimeUnit.HOURS);
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeout3(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                transaction.setCommitTimeout(1, TimeUnit.HOURS);

                // do sql
            });
        }
    }
}
