package com.tsurugidb.iceaxe.transaction.option;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;

/**
 * Tsurugi Transaction Option (common)
 *
 * @param <T> concrete class
 */
@ThreadSafe
public abstract class AbstractTgTxOption<T extends AbstractTgTxOption<T>> implements TgTxOption {

    private String label = null;

    private TransactionOption lowTransactionOption;

    protected final T self() {
        @SuppressWarnings("unchecked")
        var r = (T) this;
        return r;
    }

    @Override
    public synchronized T label(String label) {
        this.label = label;
        resetTransactionOption();
        return self();
    }

    @Override
    public synchronized String label() {
        return this.label;
    }

    protected final void resetTransactionOption() {
        this.lowTransactionOption = null;
    }

    @Override
    public synchronized TransactionOption toLowTransactionOption() {
        if (this.lowTransactionOption == null) {
            var lowBuilder = TransactionOption.newBuilder();
            initializeLowTransactionOption(lowBuilder);
            this.lowTransactionOption = lowBuilder.build();
        }
        return this.lowTransactionOption;
    }

    @OverridingMethodsMustInvokeSuper
    protected void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        var lowType = type();
        lowBuilder.setType(lowType);

        if (this.label != null) {
            lowBuilder.setLabel(label);
        }
    }

    @Override
    public T clone() {
        T txOption;
        try {
            @SuppressWarnings("unchecked")
            var clone = (T) super.clone();
            txOption = clone;
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }

        txOption.resetTransactionOption();
        return txOption;
    }

    @Override
    public T clone(String label) {
        T txOption = clone();
        txOption.label(label);
        return txOption;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(128);
        sb.append(typeName());
        sb.append("{");

        toString(sb);

        sb.append("}");
        return sb.toString();
    }

    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        appendString(sb, "label", label);
    }

    protected final void appendString(StringBuilder sb, String name, Object value) {
        if (value != null) {
            if (sb.charAt(sb.length() - 1) != '{') {
                sb.append(", ");
            }
            sb.append(name);
            sb.append('=');
            sb.append(value);
        }
    }
}
