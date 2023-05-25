package com.tsurugidb.iceaxe.transaction.option;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

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
public abstract class AbstractTgTxOptionLong<T extends AbstractTgTxOptionLong<T>> extends AbstractTgTxOption<T> {

    private TransactionPriority lowPriority = null;
    private List<String> inclusiveReadAreaList = new ArrayList<>();
    private List<String> exclusiveReadAreaList = new ArrayList<>();

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

    /**
     * add inclusive read area
     *
     * @param tableName table name
     * @return this
     */
    public synchronized T addInclusiveReadArea(String tableName) {
        inclusiveReadAreaList.add(tableName);
        resetTransactionOption();
        return self();
    }

    /**
     * add inclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized T addInclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return self();
    }

    /**
     * add inclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized T addInclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return self();
    }

    /**
     * add inclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized T addInclusiveReadArea(Stream<String> tableNames) {
        tableNames.forEachOrdered(inclusiveReadAreaList::add);
        resetTransactionOption();
        return self();
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
    public synchronized T addExclusiveReadArea(String tableName) {
        exclusiveReadAreaList.add(tableName);
        resetTransactionOption();
        return self();
    }

    /**
     * add exclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized T addExclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return self();
    }

    /**
     * add exclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized T addExclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return self();
    }

    /**
     * add exclusive read area
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized T addExclusiveReadArea(Stream<String> tableNames) {
        tableNames.forEachOrdered(exclusiveReadAreaList::add);
        resetTransactionOption();
        return self();
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
    public synchronized T clone() {
        T txOption = super.clone();
        var cast = (AbstractTgTxOptionLong<T>) txOption;
        cast.inclusiveReadAreaList = new ArrayList<>(this.inclusiveReadAreaList);
        cast.exclusiveReadAreaList = new ArrayList<>(this.exclusiveReadAreaList);
        return txOption;
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
