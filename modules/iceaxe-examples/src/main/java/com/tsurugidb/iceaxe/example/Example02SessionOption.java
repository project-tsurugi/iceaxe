package com.tsurugidb.iceaxe.example;

import java.util.concurrent.TimeUnit;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.TgCommitType;

/**
 * TgSessionOption example
 *
 * @see Example02Session
 */
public class Example02SessionOption {

    void timeout() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);
    }

    void commitType() {
        var sessionOption = TgSessionOption.of();
        sessionOption.setCommitType(TgCommitType.DEFAULT);
    }
}
