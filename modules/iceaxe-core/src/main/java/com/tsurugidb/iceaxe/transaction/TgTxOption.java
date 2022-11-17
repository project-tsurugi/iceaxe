package com.tsurugidb.iceaxe.transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;

/**
 * Tsurugi Transaction Option
 */
@ThreadSafe
public class TgTxOption implements Cloneable {

    /**
     * create transaction option
     *
     * @return transaction option
     */
    public static TgTxOption of() {
        return new TgTxOption();
    }

    /**
     * create transaction option for OCC
     *
     * @return transaction option
     */
    public static TgTxOption ofOCC() {
        return of().type(TransactionType.SHORT);
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOption ofLTX(String... writePreserveTableNames) {
        // return ofLTX(List.of(writePreserveTableNames));
        var option = of().type(TransactionType.LONG);
        for (var name : writePreserveTableNames) {
            option.addWritePreserve(name);
        }
        return option;
    }

    /**
     * create transaction option for long transaction
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOption ofLTX(Collection<String> writePreserveTableNames) {
        var option = of().type(TransactionType.LONG);
        for (var name : writePreserveTableNames) {
            option.addWritePreserve(name);
        }
        return option;
    }

    /**
     * create transaction option for read only transaction
     *
     * @return transaction option
     */
    public static TgTxOption ofRTX() {
        return of().type(TransactionType.READ_ONLY);
    }

    private TransactionType lowType = TransactionType.TRANSACTION_TYPE_UNSPECIFIED;
    private List<String> writePreserveList = null;
    private String label = null;
    private TransactionPriority lowPriority = TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED;
    private TransactionOption lowTransactionOption;

    /**
     * Tsurugi Transaction Option
     */
    public TgTxOption() {
        // do nothing
    }

    /**
     * set transaction type
     *
     * @param type transaction type
     * @return this
     */
    public synchronized TgTxOption type(TransactionType type) {
        this.lowType = type;
        reset();
        return this;
    }

    /**
     * get transaction type
     *
     * @return transaction type
     */
    public synchronized TransactionType type() {
        return this.lowType;
    }

    /**
     * add write preserve
     *
     * @param tableName table name
     * @return this
     */
    public synchronized TgTxOption addWritePreserve(String tableName) {
        if (this.writePreserveList == null) {
            this.writePreserveList = new ArrayList<>();
        }
        writePreserveList.add(tableName);
        reset();
        return this;
    }

    /**
     * get write preserve
     *
     * @return list of table name
     */
    public synchronized List<String> writePreserve() {
        if (this.writePreserveList == null) {
            return List.of();
        }
        return this.writePreserveList;
    }

    /**
     * set label
     *
     * @param label label
     * @return this
     */
    public synchronized TgTxOption label(String label) {
        this.label = label;
        reset();
        return this;
    }

    /**
     * get label
     *
     * @return label
     */
    public synchronized String label() {
        return this.label;
    }

    /**
     * set priority
     *
     * @param priority priority
     * @return this
     */
    public synchronized TgTxOption priority(TransactionPriority priority) {
        this.lowPriority = priority;
        reset();
        return this;
    }

    /**
     * get priority
     *
     * @return priority
     */
    public synchronized TransactionPriority priority() {
        return this.lowPriority;
    }

    protected final void reset() {
        this.lowTransactionOption = null;
    }

    // internal
    public synchronized TransactionOption toLowTransactionOption() {
        if (this.lowTransactionOption == null) {
            var lowBuilder = TransactionOption.newBuilder();
            initializeLowTransactionOption(lowBuilder);
            this.lowTransactionOption = lowBuilder.build();
        }
        return this.lowTransactionOption;
    }

    protected void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        if (lowType != null) {
            lowBuilder.setType(lowType);
        }
        if (writePreserveList != null) {
            for (String name : writePreserveList) {
                var value = WritePreserve.newBuilder().setTableName(name).build();
                lowBuilder.addWritePreserves(value);
            }
        }
        if (label != null) {
            lowBuilder.setLabel(label);
        }
        if (lowPriority != null) {
            lowBuilder.setPriority(lowPriority);
        }
    }

    @Override
    public TgTxOption clone() {
        TgTxOption option;
        try {
            option = (TgTxOption) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }

        if (option.writePreserveList != null) {
            option.writePreserveList = new ArrayList<>(option.writePreserveList);
        }

        option.reset();
        return option;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(128);

        sb.append(getTransactionTypeName(lowType));

        sb.append("{label=");
        sb.append(label);

        sb.append(", priority=");
        sb.append(getTransactionPriorityName(lowPriority));

        sb.append(", writePreserve=");
        sb.append(writePreserveList);

        sb.append("}");
        return sb.toString();
    }

    private String getTransactionTypeName(TransactionType lowType) {
        if (lowType == null) {
            var name = getClass().getSimpleName();
            if (name.isEmpty()) {
                name = TgTxOption.class.getSimpleName();
            }
            return name;
        }
        switch (lowType) {
        case SHORT:
            return "OCC";
        case LONG:
            return "LTX";
        case READ_ONLY:
            return "RTX";
        case TRANSACTION_TYPE_UNSPECIFIED:
            return "DEFAULT";
        default:
            return lowType.name();
        }
    }

    private static String getTransactionPriorityName(TransactionPriority lowPriority) {
        if (lowPriority == null) {
            return "null";
        }
        switch (lowPriority) {
        case TRANSACTION_PRIORITY_UNSPECIFIED:
            return "DEFUALT";
        default:
            return lowPriority.name();
        }
    }
}
