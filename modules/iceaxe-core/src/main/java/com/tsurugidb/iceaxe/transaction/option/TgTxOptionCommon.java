package com.tsurugidb.iceaxe.transaction.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;

/**
 * Tsurugi Transaction Option (common)
 *
 * @param <T> concrete class
 */
@ThreadSafe
public abstract class TgTxOptionCommon<T extends TgTxOptionCommon<?>> implements TgTxOption {

    private String label = null;
    private TransactionPriority lowPriority = null;

    private TransactionOption lowTransactionOption;

    @Override
    @SuppressWarnings("unchecked")
    public synchronized T label(String label) {
        this.label = label;
        reset();
        return (T) this;
    }

    @Override
    public synchronized String label() {
        return this.label;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized T priority(TransactionPriority priority) {
        this.lowPriority = priority;
        reset();
        return (T) this;
    }

    @Override
    public synchronized TransactionPriority priority() {
        return this.lowPriority;
    }

    protected final void reset() {
        this.lowTransactionOption = null;
    }

    @Override
    public synchronized TransactionOption toLowTransactionOption() {
        if (this.lowTransactionOption == null) {
            var lowBuilder = TransactionOption.newBuilder();
            initializeLowTransactionOptionCommon(lowBuilder);
            initializeLowTransactionOption(lowBuilder);
            this.lowTransactionOption = lowBuilder.build();
        }
        return this.lowTransactionOption;
    }

    private void initializeLowTransactionOptionCommon(TransactionOption.Builder lowBuilder) {
        var lowType = type();
        lowBuilder.setType(lowType);

        if (label != null) {
            lowBuilder.setLabel(label);
        }
        if (lowPriority != null) {
            lowBuilder.setPriority(lowPriority);
        }
    }

    protected void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        // do override
    }

    @Override
    @SuppressWarnings("unchecked")
    public T clone() {
        T option;
        try {
            option = (T) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }

        option.reset();
        return option;
    }

    @Override
    public T clone(String label) {
        T option = clone();
        option.label(label);
        return option;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(128);
        sb.append(typeName());
        sb.append("{");

        appendString(sb, "label", label);
        appendString(sb, "priority", getTransactionPriorityName(lowPriority));
        toString(sb);

        sb.append("}");
        return sb.toString();
    }

    protected void toString(StringBuilder sb) {
        // do override
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

    private static String getTransactionPriorityName(TransactionPriority lowPriority) {
        if (lowPriority == null) {
            return null;
        }
        switch (lowPriority) {
        case TRANSACTION_PRIORITY_UNSPECIFIED:
            return "DEFAULT";
        default:
            return lowPriority.name();
        }
    }
}
