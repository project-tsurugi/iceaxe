package com.tsurugidb.iceaxe.test.connector;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * connector test
 */
class DbConnectorTest extends DbTestTableTester {

    @Test
    void doNothing() throws Exception {
        try (var socket = DbTestConnector.createSocket()) {
            // do nothing
        }

        // tateyama-serverが落ちていなければ、正常に動作する
        var session = getSession();
        session.findTableMetadata(TEST);
    }
}
