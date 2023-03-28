package com.tsurugidb.iceaxe.transaction.option;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;

/**
 * Tsurugi Transaction Option (long transaction)
 *
 * @param <T> concrete class
 */
@ThreadSafe
public abstract class AbstractTgTxOptionLong<T extends AbstractTgTxOptionLong<?>> extends AbstractTgTxOption<T> {

    private TransactionPriority lowPriority = null;
    private List<String> inclusiveReadAreaList = new ArrayList<>();
    private List<String> exclusiveReadAreaList = new ArrayList<>();

    /**
     * set priority
     *
     * @param priority priority
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T priority(TransactionPriority priority) {
        this.lowPriority = priority;
        reset();
        return (T) this;
    }

    /**
     * get priority
     *
     * @return priority
     */
    public synchronized TransactionPriority priority() {
        return this.lowPriority;
    }

    /**
     * add inclusive read area
     *
     * @param tableName table name
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T addInclusiveReadArea(String tableName) {
        inclusiveReadAreaList.add(tableName);
        reset();
        return (T) this;
    }

    /**
     * add inclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T addInclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.add(tableName);
        }
        reset();
        return (T) this;
    }

    /**
     * add inclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T addInclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.add(tableName);
        }
        reset();
        return (T) this;
    }

    /**
     * get inclusive read area
     *
     * @return inclusive read area
     */
    public List<String> inclusiveReadArea() {
        return this.inclusiveReadAreaList;
    }

    /**
     * add exclusive read area
     *
     * @param tableName table name
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T addExclusiveReadArea(String tableName) {
        exclusiveReadAreaList.add(tableName);
        reset();
        return (T) this;
    }

    /**
     * add exclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T addExclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.add(tableName);
        }
        reset();
        return (T) this;
    }

    /**
     * add exclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    @SuppressWarnings("unchecked")
    public synchronized T addExclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.add(tableName);
        }
        reset();
        return (T) this;
    }

    /**
     * get exclusive read area
     *
     * @return exclusive read area
     */
    public List<String> exclusiveReadArea() {
        return this.exclusiveReadAreaList;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected synchronized void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        super.initializeLowTransactionOption(lowBuilder);

        if (this.lowPriority != null) {
            lowBuilder.setPriority(lowPriority);
        }
        for (String name : inclusiveReadAreaList) {
            var value = ReadArea.newBuilder().setTableName(name).build();
            lowBuilder.addInclusiveReadAreas(value);
        }
        for (String name : exclusiveReadAreaList) {
            var value = ReadArea.newBuilder().setTableName(name).build();
            lowBuilder.addExclusiveReadAreas(value);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T clone() {
        var txOption = (AbstractTgTxOptionLong<?>) super.clone();
        txOption.inclusiveReadAreaList = new ArrayList<>(this.inclusiveReadAreaList);
        txOption.exclusiveReadAreaList = new ArrayList<>(this.exclusiveReadAreaList);
        return (T) txOption;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        if (this.lowPriority != null) {
            appendString(sb, "priority", getTransactionPriorityName(lowPriority));
        }
        if (!inclusiveReadAreaList.isEmpty()) {
            appendString(sb, "inclusiveReadArea", inclusiveReadAreaList);
        }
        if (!exclusiveReadAreaList.isEmpty()) {
            appendString(sb, "exclusiveReadArea", exclusiveReadAreaList);
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
