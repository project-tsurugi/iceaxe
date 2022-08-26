package com.tsurugidb.iceaxe.metadata;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;

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
        try {
            LOG.trace("getTableMetadata started");
            var info = session.getSessionInfo();
            var closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.METADATA_CLOSE);
            closeTimeout.apply(lowTableMetadataFuture);

            var connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.METADATA_CONNECT);
            var lowTableMetadata = IceaxeIoUtil.getFromFuture(lowTableMetadataFuture, connectTimeout);
            LOG.trace("getTableMetadata end");

            return Optional.of(new TsurugiTableMetadata(lowTableMetadata));
        } catch (TsurugiIOException e) {
            var code = e.getLowDiagnosticCode();
            if (code == SqlServiceCode.ERR_NOT_FOUND) {
                LOG.trace("getTableMetadata end (tableName={} not found)", tableName);
                return Optional.empty();
            }
            throw e;
        } finally {
            IceaxeIoUtil.close(lowTableMetadataFuture);
        }
    }
}
