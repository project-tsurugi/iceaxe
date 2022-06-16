package com.tsurugidb.iceaxe.result;

import java.io.IOException;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.jogasaki.proto.SqlResponse.ResultOnly;

/**
 * Tsurugi Result Count for PreparedStatement
 */
public class TsurugiResultCount extends TsurugiResult {

    private FutureResponse<ResultOnly> lowResultOnlyFuture;

    // internal
    public TsurugiResultCount(TsurugiTransaction transaction, FutureResponse<ResultOnly> lowResultOnlyFuture) {
        super(transaction);
        this.lowResultOnlyFuture = lowResultOnlyFuture;
    }

    @Override
    protected FutureResponse<ResultOnly> getLowResultOnlyFuture() throws IOException {
        return lowResultOnlyFuture;
    }

    /**
     * get count
     * 
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int getUpdateCount() throws IOException, TsurugiTransactionException {
        checkResultStatus(false);
        // FIXME 更新件数取得
//      throw new InternalError("not yet implements");
        System.err.println("not yet implements TsurugiResultCount.getUpdateCount(), now always returns -1");
        return -1;
    }

    @Override
    public void close() throws IOException {
        // TODO checkResultStatusが廃止されたら、コメントも削除
        // checkResultStatus(true); クローズ時にはステータスチェックは行わない
        // 一度も更新件数を取得しない場合でも、commitでステータスチェックされる

        // not try-finally
        IceaxeIoUtil.close(lowResultOnlyFuture);
        super.close();
    }
}
