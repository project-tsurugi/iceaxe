package com.tsurugi.iceaxe.result;

import java.io.IOException;
import java.util.concurrent.Future;

import com.nautilus_technologies.tsubakuro.protos.ResponseProtos.ResultOnly;
import com.tsurugi.iceaxe.statement.TsurugiPreparedStatement;

/**
 * Tsurugi Result Count for PreparedStatement
 */
public class TsurugiResultCount extends TsurugiResult {

    private Future<ResultOnly> lowResultOnlyFuture;

    // internal
    public TsurugiResultCount(TsurugiPreparedStatement preparedStatement, Future<ResultOnly> lowResultOnlyFuture) {
        super(preparedStatement);
        this.lowResultOnlyFuture = lowResultOnlyFuture;
    }

    @Override
    protected Future<ResultOnly> getLowResultOnlyFuture() throws IOException {
        return lowResultOnlyFuture;
    }

    /**
     * get count
     * 
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException
     */
    public int getUpdateCount() throws IOException {
        // FIXME ステータスがRESULT_NOT_SETだったらどうする？
        checkResultStatus(false);
        // FIXME 更新件数取得
        throw new InternalError("not yet implements");
    }

    @Override
    public void close() throws IOException {
        // checkResultStatus(true); クローズ時にはステータスチェックは行わない
        // 一度も更新件数を取得しない場合でも、commitでステータスチェックされる
        super.close();
    }
}
