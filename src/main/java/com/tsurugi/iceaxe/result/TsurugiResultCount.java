package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatement;

/**
 * Tsurugi Result Count for PreparedStatement
 */
public class TsurugiResultCount extends TsurugiResult {

    // internal
    public TsurugiResultCount(TsurugiPreparedStatement preparedStatement, Future<ResultOnly> lowResultOnlyFuture) {
        super(preparedStatement, lowResultOnlyFuture);
    }

    /**
     * get count
     * 
     * @return the row count for SQL Data Manipulation Language (DML) statements
     */
    public int getUpdateCount() {
        // TODO 更新件数取得
        throw new InternalError("not yet implements");
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}
