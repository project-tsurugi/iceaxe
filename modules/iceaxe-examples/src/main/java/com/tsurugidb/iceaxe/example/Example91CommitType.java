package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * example to specify commitType
 */
public class Example91CommitType {

    void commitTypeBySessionOption(TsurugiConnector connector) throws IOException, InterruptedException {
        var sessionOption = TgSessionOption.of();
        sessionOption.setCommitType(TgCommitType.STORED);

        try (var session = connector.createSession(sessionOption)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commitTypeByCreateTm(TsurugiConnector connector) throws IOException, InterruptedException {
        try (var session = connector.createSession()) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitType(TgCommitType.STORED);
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commitTypeByTmExecute(TsurugiConnector connector) throws IOException, InterruptedException {
        try (var session = connector.createSession()) {
            var tm = session.createTransactionManager();

            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitType(TgCommitType.STORED);
            tm.execute(setting, transaction -> {
                // do sql
            });
        }
    }
}
