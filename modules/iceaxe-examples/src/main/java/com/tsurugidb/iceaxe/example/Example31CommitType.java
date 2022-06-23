package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * example to specify commitType
 */
public class Example31CommitType {

    void main() throws IOException {
        var connector = TsurugiConnector.createConnector("tcp://localhost:12345");
        commit1(connector);
        commit2(connector);
        commit3(connector);
    }

    void commit1(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");
        info.commitType(TgCommitType.STORED);

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC());
            var tm = session.createTransactionManager(setting);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commit2(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitType(TgCommitType.STORED);
            var tm = session.createTransactionManager(setting);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commit3(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager();

            var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitType(TgCommitType.STORED);
            tm.execute(setting, transaction -> {
                // do sql
            });
        }
    }
}
