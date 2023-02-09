package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiSqlPrepared;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * PreparedStatement connect timeout test
 */
public class DbTimeoutPsConnectTest extends DbTimetoutTest {

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
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.PS_CONNECT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyPs(TsurugiSqlPrepared<?> ps) {
                ps.setConnectTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.getLowTransaction();

            pipeServer.setPipeWrite(false);
            try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
                modifier.modifyPs(ps);

                var entity = createTestEntity(0);
                try {
                    ps.execute(transaction, entity);
                } catch (IOException e) {
                    assertInstanceOf(TimeoutException.class, e.getCause());
                    LOG.trace("timeout success");
                    return;
                } finally {
                    pipeServer.setPipeWrite(true);
                }
                fail("didn't time out");
            }
        }
    }
}
