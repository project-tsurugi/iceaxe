package com.tsurugidb.iceaxe.sql.explain;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi statement metadata helper
 */
public class TsurugiExplainHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiExplainHelper.class);

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session tsurugi session
     * @param source  SQL
     * @return statement metadata
     * @throws IOException
     * @throws InterruptedException
     */
    public TgStatementMetadata explain(TsurugiSession session, String source) throws IOException, InterruptedException {
        var sessionOption = session.getSessionOption();
        var connectTimeout = getConnectTimeout(sessionOption);
        var closeTimeout = getCloseTimeout(sessionOption);
        return explain(session, source, connectTimeout, closeTimeout);
    }

    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.EXPLAIN_CONNECT);
    }

    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.EXPLAIN_CLOSE);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session        tsurugi session
     * @param source         SQL
     * @param connectTimeout connect timeout
     * @param closeTimeout   close timeout
     * @return statement metadata
     * @throws IOException
     * @throws InterruptedException
     */
    public TgStatementMetadata explain(TsurugiSession session, String source, IceaxeTimeout connectTimeout, IceaxeTimeout closeTimeout) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("explain start. source={}", source);
        var lowStatementMetadataFuture = explainLow(lowSqlClient, source);
        LOG.trace("explain started");
        return getStatementMetadata(session, source, null, lowStatementMetadataFuture, connectTimeout, closeTimeout);
    }

    protected FutureResponse<StatementMetadata> explainLow(SqlClient lowSqlClient, String source) throws IOException {
        return lowSqlClient.explain(source);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session        tsurugi session
     * @param source         SQL
     * @param arguments      SQL arguments
     * @param lowPs          PreparedStatement
     * @param lowParameter   list of Parameter
     * @param connectTimeout connect timeout
     * @param closeTimeout   close timeout
     * @return statement metadata
     * @throws IOException
     * @throws InterruptedException
     */
    public TgStatementMetadata explain(TsurugiSession session, String source, Object arguments, PreparedStatement lowPs, List<Parameter> lowParameter, IceaxeTimeout connectTimeout,
            IceaxeTimeout closeTimeout) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("explain start. source={}", source);
        var lowStatementMetadataFuture = explainLow(lowSqlClient, lowPs, lowParameter);
        LOG.trace("explain started");
        return getStatementMetadata(session, source, arguments, lowStatementMetadataFuture, connectTimeout, closeTimeout);
    }

    protected FutureResponse<StatementMetadata> explainLow(SqlClient lowSqlClient, PreparedStatement lowPs, List<Parameter> lowParameter) throws IOException {
        return lowSqlClient.explain(lowPs, lowParameter);
    }

    protected TgStatementMetadata getStatementMetadata(TsurugiSession session, String source, Object arguments, FutureResponse<StatementMetadata> lowStatementMetadataFuture,
            IceaxeTimeout connectTimeout, IceaxeTimeout closeTimeout) throws IOException, InterruptedException {
        try (var closeable = IceaxeIoUtil.closeable(lowStatementMetadataFuture)) {
            closeTimeout.apply(lowStatementMetadataFuture);

            var lowStatementMetadata = IceaxeIoUtil.getAndCloseFuture(lowStatementMetadataFuture, connectTimeout);
            LOG.trace("explain end");

            return newStatementMetadata(source, arguments, lowStatementMetadata);
        }
    }

    protected TgStatementMetadata newStatementMetadata(String source, Object arguments, StatementMetadata lowStatementMetadata) {
        return new TgStatementMetadata(source, arguments, lowStatementMetadata);
    }
}
