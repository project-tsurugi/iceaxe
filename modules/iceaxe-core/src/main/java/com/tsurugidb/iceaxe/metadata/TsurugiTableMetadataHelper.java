package com.tsurugidb.iceaxe.metadata;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi table metadata helper
 */
public class TsurugiTableMetadataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTableMetadataHelper.class);

    private TsurugiExceptionUtil exceptionUtil = TsurugiExceptionUtil.getInstance();

    /**
     * set exception utility
     *
     * @param execptionUtil exception utility
     */
    public void setExceptionUtil(@Nonnull TsurugiExceptionUtil execptionUtil) {
        this.exceptionUtil = Objects.requireNonNull(execptionUtil);
    }

    protected TsurugiExceptionUtil getExceptionUtil() {
        return this.exceptionUtil;
    }

    /**
     * get table metadata.
     *
     * @param session   tsurugi session
     * @param tableName table name
     * @return table metadata
     * @throws IOException
     * @throws InterruptedException
     */
    public Optional<TgTableMetadata> findTableMetadata(TsurugiSession session, String tableName) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("getTableMetadata start. tableName={}", tableName);
        var lowTableMetadataFuture = getLowTableMetadata(lowSqlClient, tableName);
        LOG.trace("getTableMetadata started");
        return findTableMetadata(session, tableName, lowTableMetadataFuture);
    }

    protected FutureResponse<TableMetadata> getLowTableMetadata(SqlClient lowSqlClient, String tableName) throws IOException {
        return lowSqlClient.getTableMetadata(tableName);
    }

    protected Optional<TgTableMetadata> findTableMetadata(TsurugiSession session, String tableName, FutureResponse<TableMetadata> lowTableMetadataFuture) throws IOException, InterruptedException {
        try (var closeable = IceaxeIoUtil.closeable(lowTableMetadataFuture)) {

            var sessionOption = session.getSessionOption();
            var connectTimeout = getConnectTimeout(sessionOption);
            var closeTimeout = getCloseTimeout(sessionOption);
            closeTimeout.apply(lowTableMetadataFuture);

            var lowTableMetadata = IceaxeIoUtil.getAndCloseFuture(lowTableMetadataFuture, connectTimeout);
            LOG.trace("getTableMetadata end");

            return Optional.of(newTableMetadata(lowTableMetadata));
        } catch (TsurugiIOException e) {
            if (exceptionUtil.isTargetNotFound(e)) {
                LOG.trace("getTableMetadata end (tableName={} not found)", tableName);
                return Optional.empty();
            }
            throw e;
        }
    }

    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.METADATA_CONNECT);
    }

    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.METADATA_CLOSE);
    }

    protected TgTableMetadata newTableMetadata(TableMetadata lowTableMetadata) {
        return new TgTableMetadata(lowTableMetadata);
    }
}
