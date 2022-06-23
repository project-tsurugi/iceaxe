package com.tsurugidb.iceaxe.example;

import java.io.IOException;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgTxOptionList;

/**
 * example to specify commitType
 */
public class Example31CommitType {

    private static final TgTxOptionList TX_OPTION = TgTxOptionList.of(TgTxOption.ofOCC());

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
            var tm = session.createTransactionManager(TX_OPTION);
            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commit2(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager(TX_OPTION, TgCommitType.STORED);

            tm.execute(transaction -> {
                // do sql
            });
        }
    }

    void commit3(TsurugiConnector connector) throws IOException {
        var info = TgSessionInfo.of("user", "password");

        try (var session = connector.createSession(info)) {
            var tm = session.createTransactionManager();

            tm.execute(TX_OPTION, TgCommitType.STORED, transaction -> {
                // do sql
            });
        }
    }
}
