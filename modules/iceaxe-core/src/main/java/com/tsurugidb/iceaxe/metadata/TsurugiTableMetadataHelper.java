package com.tsurugidb.iceaxe.metadata;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * Tsurugi table metadata helper
 */
public final class TsurugiTableMetadataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTableMetadataHelper.class);

    private TsurugiTableMetadataHelper() {
        // don't instantiate
    }

    // internal
    public static Optional<TsurugiTableMetadata> findTableMetadata(TsurugiSession session, String tableName) throws IOException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("getTableMetadata start. tableName={}", tableName);
        var lowTableMetadataFuture = lowSqlClient.getTableMetadata(tableName);
        LOG.trace("getTableMetadata started");
        try (var closeable = IceaxeIoUtil.closeable(lowTableMetadataFuture)) {

            var info = session.getSessionInfo();
            var connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.METADATA_CONNECT);
            var closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.METADATA_CLOSE);
            closeTimeout.apply(lowTableMetadataFuture);

            var lowTableMetadata = IceaxeIoUtil.getAndCloseFuture(lowTableMetadataFuture, connectTimeout);
            LOG.trace("getTableMetadata end");

            return Optional.of(new TsurugiTableMetadata(lowTableMetadata));
        } catch (TsurugiIOException e) {
            var code = e.getLowDiagnosticCode();
            if (code == SqlServiceCode.ERR_NOT_FOUND) {
                LOG.trace("getTableMetadata end (tableName={} not found)", tableName);
                return Optional.empty();
            }
            throw e;
        }
    }
}
