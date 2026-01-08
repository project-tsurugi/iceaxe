package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionShutdownType;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * session shutdown test
 */
class DbSessionShutdownTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSessionShutdownTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @ParameterizedTest
    @ValueSource(strings = { "N-N", "N-G", "N-F", "G-N", "G-G", "G-F", "F-N", "F-G", "F-F" })
    void shutdown_noGetLowSession(String pattern) throws Exception {
        testShutdown(pattern, false);
    }

    @ParameterizedTest
    @ValueSource(strings = { "N-N", "N-G", "N-F", "G-N", "G-G", "G-F", "F-N", "F-G", "F-F" })
    void shutdown(String pattern) throws Exception {
        testShutdown(pattern, true);
    }

    private void testShutdown(String pattern, boolean initialize) throws IOException, InterruptedException {
        var closeShutdownType = shutdownType(pattern.charAt(0));
        var shutdownType = shutdownType(pattern.charAt(2));
        try (var session = DbTestConnector.createSession(closeShutdownType)) {
            if (initialize) {
                assertTrue(session.isAlive());
            }

            session.shutdown(shutdownType, 10, TimeUnit.SECONDS);

            if (shutdownType == TgSessionShutdownType.NOTHING) {
                assertTrue(session.isAlive());
            } else {
                assertFalse(session.isAlive());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "G-G", "G-F", "F-G", "F-F" })
    void shutdownTwice(String pattern) throws Exception {
        var shutdownType1 = shutdownType(pattern.charAt(0));
        var shutdownType2 = shutdownType(pattern.charAt(2));
        try (var session = DbTestConnector.createSession(TgSessionShutdownType.NOTHING)) {
            session.shutdown(shutdownType1, 10, TimeUnit.SECONDS);
            session.shutdown(shutdownType2, 10, TimeUnit.SECONDS);
        }
    }

    private static TgSessionShutdownType shutdownType(char c) {
        switch (c) {
        case 'N':
            return TgSessionShutdownType.NOTHING;
        case 'G':
            return TgSessionShutdownType.GRACEFUL;
        case 'F':
            return TgSessionShutdownType.FORCEFUL;
        default:
            throw new AssertionError(c);
        }
    }
}
