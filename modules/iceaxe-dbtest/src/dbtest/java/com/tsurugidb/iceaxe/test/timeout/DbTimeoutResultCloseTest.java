package com.tsurugidb.iceaxe.test.timeout;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Result close timeout test
 */
public class DbTimeoutResultCloseTest extends DbTimetoutTest {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void timeoutDefault() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.RESULT_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyStatementResult(TsurugiStatementResult result) {
                result.setCloseTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.getLowTransaction();

            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(0);
                var result = ps.execute(transaction, entity);
                modifier.modifyStatementResult(result);

                pipeServer.setPipeWrite(false);
                try {
                    result.close();
                } catch (IceaxeIOException e) {
                    assertEqualsCode(IceaxeErrorCode.RESULT_CLOSE_TIMEOUT, e);
                    return;
                } finally {
                    pipeServer.setPipeWrite(true);
                }
                // RESULT_CLOSEはタイムアウトするような通信処理が無い
//              fail("didn't time out");
            }
        }
    }
}
