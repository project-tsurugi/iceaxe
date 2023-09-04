package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.FutureResponseCloseWrapper;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiStatementResult} test
 */
class DbStatementResultTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void constructorError() throws Exception {
        var session = getSession();
        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var future = FutureResponseCloseWrapper.of(transaction.getLowTransaction().executeStatement(ps.getSql()));

                transaction.close();
                var e = assertThrows(TsurugiIOException.class, () -> new TsurugiStatementResult(0, transaction, ps, null, future));
                assertEqualsCode(IceaxeErrorCode.TX_ALREADY_CLOSED, e);
                assertTrue(future.isClosed());
            }
        }
    }
}
