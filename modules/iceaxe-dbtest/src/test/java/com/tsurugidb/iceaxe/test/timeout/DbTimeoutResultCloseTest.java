package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
                } catch (IOException e) {
                    // RESULT_CLOSEはタイムアウトするような通信処理が無い
//                  assertInstanceOf(TimeoutException.class, e.getCause());
//                  LOG.trace("timeout success");
//                  return;
                    throw e;
                } finally {
                    pipeServer.setPipeWrite(true);
                }
//              fail("didn't time out");
            }
        }
    }
}
