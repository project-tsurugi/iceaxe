package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * table metadata connect timeout test
 */
@Disabled // TODO remove Disabled
public class DbTimeoutTableMetadataConnectTest extends DbTimetoutTest {

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
                info.timeout(TgTimeoutKey.METADATA_CONNECT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var helper = new TsurugiTableMetadataHelper() {
            @Override
            protected FutureResponse<TableMetadata> getLowTableMetadata(SqlClient lowSqlClient, String tableName) throws IOException {
                pipeServer.setPipeWrite(false);
                return super.getLowTableMetadata(lowSqlClient, tableName);
            }
        };
        setHelper(session, helper);

        try {
            session.findTableMetadata(TEST);
        } catch (IOException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
            LOG.trace("timeout success");
            return;
        } finally {
            pipeServer.setPipeWrite(true);
        }
        fail("didn't time out");
    }

    static void setHelper(TsurugiSession session, TsurugiTableMetadataHelper helper) {
        try {
            var field = session.getClass().getDeclaredField("tableMetadataHelper");
            field.setAccessible(true);
            field.set(session, helper);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
