package com.tsurugidb.iceaxe.result;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Result Count for PreparedStatement
 */
@NotThreadSafe
public class TsurugiResultCount extends TsurugiResult {

    private FutureResponse<Void> lowResultFuture;

    // internal
    public TsurugiResultCount(TsurugiTransaction transaction, FutureResponse<Void> lowResultFuture) {
        super(transaction);
        this.lowResultFuture = lowResultFuture;
    }

    @Override
    protected FutureResponse<Void> getLowResultFuture() throws IOException {
        return lowResultFuture;
    }

    /**
     * get count
     * 
     * @return the row count for SQL Data Manipulation Language (DML) statements
     * @throws IOException
     * @throws TsurugiTransactionException
     */
    public int getUpdateCount() throws IOException, TsurugiTransactionException {
        checkLowResult();
        // FIXME 更新件数取得
//      throw new InternalError("not yet implements");
//      System.err.println("not yet implements TsurugiResultCount.getUpdateCount(), now always returns -1");
        return -1;
    }

    @Override
    public void close() throws IOException {
        // not try-finally
        IceaxeIoUtil.close(lowResultFuture);
        super.close();
    }
}
