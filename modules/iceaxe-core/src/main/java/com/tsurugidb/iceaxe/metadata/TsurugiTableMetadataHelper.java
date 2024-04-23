package com.tsurugidb.iceaxe.metadata;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
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
 * Tsurugi table metadata helper.
 */
public class TsurugiTableMetadataHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTableMetadataHelper.class);

    private TsurugiExceptionUtil exceptionUtil = TsurugiExceptionUtil.getInstance();

    /**
     * set exception utility.
     *
     * @param exceptionUtil exception utility
     */
    public void setExceptionUtil(@Nonnull TsurugiExceptionUtil exceptionUtil) {
        this.exceptionUtil = Objects.requireNonNull(exceptionUtil);
    }

    /**
     * get exception utility.
     *
     * @return exception utility
     */
    protected TsurugiExceptionUtil getExceptionUtil() {
        return this.exceptionUtil;
    }

    /**
     * get table metadata.
     *
     * @param session   tsurugi session
     * @param tableName table name
     * @return table metadata
     * @throws IOException          if an I/O error occurs while retrieving table metadata
     * @throws InterruptedException if interrupted while retrieving table metadata
     */
    public Optional<TgTableMetadata> findTableMetadata(TsurugiSession session, String tableName) throws IOException, InterruptedException {
        var sessionOption = session.getSessionOption();
        var connectTimeout = getConnectTimeout(sessionOption);

        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("getTableMetadata start. tableName={}", tableName);
        var lowTableMetadataFuture = getLowTableMetadata(lowSqlClient, tableName);
        LOG.trace("getTableMetadata started");
        return findTableMetadata(tableName, lowTableMetadataFuture, connectTimeout);
    }

    /**
     * get connect timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_METADATA_CONNECT);
    }

    /**
     * get close timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    @Deprecated(since = "X.X.X")
    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_METADATA_CLOSE);
    }

    /**
     * get low table metadata.
     *
     * @param lowSqlClient low SQL client
     * @param tableName    table name
     * @return future of table metadata
     * @throws IOException if an I/O error occurs while retrieving table metadata
     */
    protected FutureResponse<TableMetadata> getLowTableMetadata(SqlClient lowSqlClient, String tableName) throws IOException {
        return lowSqlClient.getTableMetadata(tableName);
    }

    /**
     * get table metadata.
     *
     * @param tableName              table name
     * @param lowTableMetadataFuture future of table metadata
     * @param connectTimeout         connect timeout
     * @return table metadata
     * @throws IOException          if an I/O error occurs while retrieving table metadata
     * @throws InterruptedException if interrupted while retrieving table metadata
     */
    protected Optional<TgTableMetadata> findTableMetadata(String tableName, FutureResponse<TableMetadata> lowTableMetadataFuture, IceaxeTimeout connectTimeout)
            throws IOException, InterruptedException {
        try {
            var lowTableMetadata = IceaxeIoUtil.getAndCloseFuture(lowTableMetadataFuture, //
                    connectTimeout, IceaxeErrorCode.TABLE_METADATA_CONNECT_TIMEOUT, //
                    IceaxeErrorCode.TABLE_METADATA_CLOSE_TIMEOUT);
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

    /**
     * Creates a new table metadata instance.
     *
     * @param lowTableMetadata low table metadata
     * @return table metadata
     */
    protected TgTableMetadata newTableMetadata(TableMetadata lowTableMetadata) {
        return new TgTableMetadata(lowTableMetadata);
    }
}
