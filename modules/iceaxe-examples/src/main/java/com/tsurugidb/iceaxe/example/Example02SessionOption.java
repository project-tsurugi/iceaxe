package com.tsurugidb.iceaxe.example;

import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.transaction.TgCommitType;

/**
 * TgSessionOption example
 *
 * @see Example02Session
 */
public class Example02SessionOption {

    void label() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setApplicationName("application name");
        sessionOption.setLabel("session label");

        // $TSURUGI_HOME/bin/tgctl session list --verbose
    }

    void keepAlive() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setKeepAlive(true);
    }

    void timeout() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);
    }

    void commitType() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setCommitType(TgCommitType.DEFAULT);
    }

    void shutdownType() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setCloseShutdownType(TgSessionShutdownType.GRACEFUL);
    }
}
