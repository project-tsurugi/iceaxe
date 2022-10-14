package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * slot limit test
 */
public class DbSlotLimitTest extends DbTimetoutTest {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void slotLimit() throws IOException {
        try {
            testTimeout(new TimeoutModifier());
        } catch (IOException e) {
            assertEquals("no available response box", e.getMessage());
        }
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.getLowTransaction();

            try (var ps = session.createPreparedQuery(SELECT_SQL)) {
                pipeServer.setPipeWrite(false);

                try {
                    int responseBoxSize = ResponseBox.responseBoxSize();
                    for (int i = 0; i < responseBoxSize + 1; i++) {
                        try {
                            ps.execute(transaction);
                        } catch (IOException e) {
                            assertEquals("no available response box", e.getMessage());
                            assertEquals(responseBoxSize, i);
                            return;
                        }
                    }
                    fail("slot limit over did not occur");
                } finally {
                    pipeServer.setPipeWrite(true);
                }
            }
        }
    }
}
