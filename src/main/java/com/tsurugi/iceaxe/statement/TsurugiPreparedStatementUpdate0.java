package com.tsurugi.iceaxe.statement;

import java.io.IOException;

import com.tsurugi.iceaxe.result.TsurugiResult;
import com.tsurugi.iceaxe.session.TsurugiSession;
import com.tsurugi.iceaxe.transaction.TsurugiTransaction;

/**
 * Tsurugi PreparedStatement
 * <ul>
 * <li>TODO+++翻訳: 更新系SQL</li>
 * <li>TODO+++翻訳: SQLのパラメーターなし</li>
 * </ul>
 */
public class TsurugiPreparedStatementUpdate0 extends TsurugiPreparedStatement {

    private final String sql;

    // internal
    public TsurugiPreparedStatementUpdate0(TsurugiSession session, String sql) {
        super(session);
        this.sql = sql;
    }

    public TsurugiResult execute(TsurugiTransaction transaction) throws IOException {
        var lowTransaction = transaction.getLowTransaction();
        var lowResultFuture = lowTransaction.executeStatement(sql);
        var result = new TsurugiResult(this, lowResultFuture);
        addChild(result);
        return result;
    }
}
