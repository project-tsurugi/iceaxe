package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

/**
 * example to specify timeout
 */
public class Example92Timeout {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        timeoutBySessionInfo(connector);
        timeoutByTmSetting(connector);
        timeoutByDirect(connector);
    }

    void timeoutBySessionInfo(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
        info.timeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeoutByTmSetting(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitTimeout(1, TimeUnit.HOURS);
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void timeoutByDirect(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);

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
