package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.result.TsurugiResultCount;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * Result close timeout test
 */
public class DbTimeoutResultCloseTest extends DbTimetoutTest {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void timeoutDefault() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.RESULT_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyResult(TsurugiResultCount result) {
                result.setCloseTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.getLowTransaction();

            try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
                var entity = createTestEntity(0);
                var result = ps.execute(transaction, entity);
                modifier.modifyResult(result);

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
