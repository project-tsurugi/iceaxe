package com.tsurugidb.iceaxe.test.connector;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * connector test
 */
class DbConnectorTest extends DbTestTableTester {

    @Test
    void label() throws Exception {
        var sessionOption = TgSessionOption.of().setLabel("test-label");

        var connector = DbTestConnector.createConnector();
        try (var session = connector.createSession(sessionOption)) {
            // TODO sessionからlabelを取得して確認したい
        }
    }

    @Test
    void applicationName() throws Exception {
        var sessionOption = TgSessionOption.of().setApplicationName("test-app");

        var connector = DbTestConnector.createConnector();
        try (var session = connector.createSession(sessionOption)) {
            // TODO sessionからapplicationNameを取得して確認したい
        }
    }

    @Test
    void doNothing() throws Exception {
        try (var socket = DbTestConnector.createSocket()) {
            // do nothing
        }

        // tsurugidbプロセスが落ちていなければ、正常に動作する
        var session = getSession();
        session.findTableMetadata(TEST);
    }
}
