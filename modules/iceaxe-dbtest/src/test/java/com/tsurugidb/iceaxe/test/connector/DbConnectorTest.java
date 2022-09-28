package com.tsurugidb.iceaxe.test.connector;

import java.io.IOException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * connector test
 */
@Disabled // TODO remove Disabled
class DbConnectorTest extends DbTestTableTester {

    @Test
    void doNothing() throws IOException {
        try (var socket = DbTestConnector.createSocket()) {
            // do nothing
        }

        // tateyama-serverが落ちていなければ、正常に動作する
        var session = getSession();
        session.findTableMetadata(TEST);
    }
}
