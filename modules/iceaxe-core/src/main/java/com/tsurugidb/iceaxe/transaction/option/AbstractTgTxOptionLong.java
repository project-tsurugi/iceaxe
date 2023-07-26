package com.tsurugidb.iceaxe.transaction.option;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;

/**
 * Tsurugi Transaction Option (long transaction)
 *
 * @param <T> concrete class
 */
@ThreadSafe
public abstract class AbstractTgTxOptionLong<T extends AbstractTgTxOptionLong<T>> extends AbstractTgTxOption<T> {

    private TransactionPriority lowPriority = null;

    /**
     * set priority
     *
     * @param priority priority
     * @return this
     */
    public synchronized T priority(TransactionPriority priority) {
        this.lowPriority = priority;
        resetTransactionOption();
        return self();
    }

    /**
     * get priority
     *
     * @return priority
     */
    public synchronized TransactionPriority priority() {
        return this.lowPriority;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected synchronized void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        super.initializeLowTransactionOption(lowBuilder);

        if (this.lowPriority != null) {
            lowBuilder.setPriority(lowPriority);
        }
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected T fillFrom(TgTxOption txOption) {
        super.fillFrom(txOption);

        if (txOption instanceof AbstractTgTxOptionLong) {
            var src = (AbstractTgTxOptionLong<?>) txOption;
            priority(src.priority());
        }

        return self();
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        if (this.lowPriority != null) {
            appendString(sb, "priority", getTransactionPriorityName(lowPriority));
        }
    }

    private static String getTransactionPriorityName(TransactionPriority lowPriority) {
        switch (lowPriority) {
        case TRANSACTION_PRIORITY_UNSPECIFIED:
            return "DEFAULT";
        default:
            return lowPriority.name();
        }
    }
}
