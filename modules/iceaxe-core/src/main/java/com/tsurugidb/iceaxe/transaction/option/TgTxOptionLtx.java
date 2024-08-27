/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.transaction.option;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.ReadArea;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;
import com.tsurugidb.sql.proto.SqlRequest.WritePreserve;

/**
 * Tsurugi Transaction Option (LTX).
 */
@ThreadSafe
public class TgTxOptionLtx extends AbstractTgTxOptionLong<TgTxOptionLtx> {

    private boolean includeDdl = false;
    private List<String> writePreserveList = new ArrayList<>();
    private List<String> inclusiveReadAreaList = new ArrayList<>();
    private List<String> exclusiveReadAreaList = new ArrayList<>();

    @Override
    public String typeName() {
        return "LTX";
    }

    @Override
    public TransactionType type() {
        return TransactionType.LONG;
    }

    /**
     * set include DDL.
     *
     * @param includeDDl {@code true} if include DDL
     * @return this
     */
    public TgTxOptionLtx includeDdl(boolean includeDDl) {
        this.includeDdl = includeDDl;
        resetTransactionOption();
        return this;
    }

    /**
     * get include DDL.
     *
     * @return {@code true} if include DDL
     */
    public boolean includeDdl() {
        return this.includeDdl;
    }

    /**
     * add write preserve.
     *
     * @param tableName table name
     * @return this
     */
    public synchronized TgTxOptionLtx addWritePreserve(String tableName) {
        writePreserveList.add(tableName);
        resetTransactionOption();
        return this;
    }

    /**
     * add write preserve.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addWritePreserve(String... tableNames) {
        for (var tableName : tableNames) {
            writePreserveList.add(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * add write preserve.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addWritePreserve(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            writePreserveList.add(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * add write preserve.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addWritePreserve(Stream<String> tableNames) {
        tableNames.forEachOrdered(writePreserveList::add);
        resetTransactionOption();
        return this;
    }

    /**
     * remove write preserve.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeWritePreserve(String... tableNames) {
        for (var tableName : tableNames) {
            writePreserveList.remove(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * remove write preserve.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeWritePreserve(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            writePreserveList.remove(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * remove write preserve.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeWritePreserve(Stream<String> tableNames) {
        tableNames.forEachOrdered(writePreserveList::remove);
        resetTransactionOption();
        return this;
    }

    /**
     * get write preserve.
     *
     * @return list of table name
     */
    public List<String> writePreserve() {
        return List.copyOf(this.writePreserveList);
    }

    /**
     * add inclusive read area.
     *
     * @param tableName table name
     * @return this
     */
    public synchronized TgTxOptionLtx addInclusiveReadArea(String tableName) {
        inclusiveReadAreaList.add(tableName);
        resetTransactionOption();
        return this;
    }

    /**
     * add inclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addInclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * add inclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addInclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * add inclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addInclusiveReadArea(Stream<String> tableNames) {
        tableNames.forEachOrdered(inclusiveReadAreaList::add);
        resetTransactionOption();
        return this;
    }

    /**
     * remove inclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeInclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.remove(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * remove inclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeInclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            inclusiveReadAreaList.remove(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * remove inclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeInclusiveReadArea(Stream<String> tableNames) {
        tableNames.forEachOrdered(inclusiveReadAreaList::remove);
        resetTransactionOption();
        return this;
    }

    /**
     * get inclusive read area.
     *
     * @return inclusive read area
     */
    public List<String> inclusiveReadArea() {
        return List.copyOf(this.inclusiveReadAreaList);
    }

    /**
     * add exclusive read area.
     *
     * @param tableName table name
     * @return this
     */
    public synchronized TgTxOptionLtx addExclusiveReadArea(String tableName) {
        exclusiveReadAreaList.add(tableName);
        resetTransactionOption();
        return this;
    }

    /**
     * add exclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addExclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * add exclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addExclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.add(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * add exclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx addExclusiveReadArea(Stream<String> tableNames) {
        tableNames.forEachOrdered(exclusiveReadAreaList::add);
        resetTransactionOption();
        return this;
    }

    /**
     * remove exclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeExclusiveReadArea(String... tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.remove(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * remove exclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeExclusiveReadArea(Collection<String> tableNames) {
        for (var tableName : tableNames) {
            exclusiveReadAreaList.remove(tableName);
        }
        resetTransactionOption();
        return this;
    }

    /**
     * remove exclusive read area.
     *
     * @param tableNames table name
     * @return this
     */
    public synchronized TgTxOptionLtx removeExclusiveReadArea(Stream<String> tableNames) {
        tableNames.forEachOrdered(exclusiveReadAreaList::remove);
        resetTransactionOption();
        return this;
    }

    /**
     * get exclusive read area.
     *
     * @return exclusive read area
     */
    public List<String> exclusiveReadArea() {
        return List.copyOf(this.exclusiveReadAreaList);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        super.initializeLowTransactionOption(lowBuilder);

        lowBuilder.setModifiesDefinitions(this.includeDdl);
        for (String name : writePreserveList) {
            var value = WritePreserve.newBuilder().setTableName(name).build();
            lowBuilder.addWritePreserves(value);
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
    public synchronized TgTxOptionLtx clone() {
        TgTxOptionLtx txOption = super.clone();
        txOption.writePreserveList = new ArrayList<>(this.writePreserveList);
        txOption.inclusiveReadAreaList = new ArrayList<>(this.inclusiveReadAreaList);
        txOption.exclusiveReadAreaList = new ArrayList<>(this.exclusiveReadAreaList);
        return txOption;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected TgTxOptionLtx fillFrom(TgTxOption txOption) {
        super.fillFrom(txOption);

        if (txOption instanceof TgTxOptionLtx) {
            var src = (TgTxOptionLtx) txOption;
            includeDdl(src.includeDdl());
            addWritePreserve(src.writePreserveList);
            addInclusiveReadArea(src.inclusiveReadAreaList);
            addExclusiveReadArea(src.exclusiveReadAreaList);
        }

        return this;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public int hashCode() {
        return super.hashCode() ^ Objects.hash(includeDdl, writePreserveList, inclusiveReadAreaList, exclusiveReadAreaList);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        if (obj instanceof TgTxOptionLtx) {
            var that = (TgTxOptionLtx) obj;
            return includeDdl == that.includeDdl() && writePreserveList.equals(that.writePreserveList) && inclusiveReadAreaList.equals(that.inclusiveReadAreaList)
                    && exclusiveReadAreaList.equals(that.exclusiveReadAreaList);
        }
        return false;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        if (this.includeDdl) {
            appendString(sb, "includeDdl", includeDdl);
            if (!writePreserveList.isEmpty()) {
                appendString(sb, "writePreserve", writePreserveList);
            }
        } else {
            appendString(sb, "writePreserve", writePreserveList);
        }
        if (!inclusiveReadAreaList.isEmpty()) {
            appendString(sb, "inclusiveReadArea", inclusiveReadAreaList);
        }
        if (!exclusiveReadAreaList.isEmpty()) {
            appendString(sb, "exclusiveReadArea", exclusiveReadAreaList);
        }
    }
}
