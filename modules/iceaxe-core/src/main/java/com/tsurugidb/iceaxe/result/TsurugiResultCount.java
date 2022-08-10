package com.tsurugidb.iceaxe.result;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nautilus_technologies.tsubakuro.util.FutureResponse;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;

/**
 * Tsurugi Result Count for PreparedStatement
 */
@NotThreadSafe
public class TsurugiResultCount extends TsurugiResult {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultCount.class);

    private FutureResponse<Void> lowResultFuture;

    // internal
    public TsurugiResultCount(TsurugiTransaction transaction, FutureResponse<Void> lowResultFuture) {
        super(transaction);
        this.lowResultFuture = lowResultFuture;
    }

    @Override
    protected FutureResponse<Void> getLowResultFutureOnce() throws IOException {
        var r = this.lowResultFuture;
        this.lowResultFuture = null;
        return r;
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
        // checkLowResult()を一度も呼ばなくても、commit()でチェックされるはず

        LOG.trace("result close start");
        // not try-finally
        IceaxeIoUtil.close(lowResultFuture);
        super.close();
        LOG.trace("result close end");
    }
}
