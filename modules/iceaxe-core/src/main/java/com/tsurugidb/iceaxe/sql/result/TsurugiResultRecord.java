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
package com.tsurugidb.iceaxe.sql.result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.IceaxeResultNameList.IceaxeAmbiguousNamePolicy;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;

/**
 * Tsurugi Result Record for {@link TsurugiQueryResult}.
 *
 * <p>
 * The methods of this class are classified into the following three groups. If you use a certain group of methods, you basically cannot use the other group's methods.
 * </p>
 * <h2><code>current column</code> group</h2>
 * <p>
 * Get the value while moving the current column by {@link #moveCurrentColumnNext()}.<br>
 * Each column value can be retrieved only once.
 * </p>
 *
 * <pre>
 * while (record.moveCurrentColumnNext()) {
 *     String name = record.getCurrentColumnName();
 *     Object value = record.fetchCurrentColumnValue();
 * }
 * </pre>
 *
 * <h2><code>name</code>/<code>index</code> group</h2>
 * <p>
 * Get the value by specifying the column name or index.
 * </p>
 *
 * <pre>
 * entity.setFoo(record.getInt("foo"));
 * entity.setBar(record.getLong("bar"));
 * entity.setZzz(record.getString("zzz"));
 * </pre>
 *
 * <pre>
 * entity.setFoo(record.getInt(0));
 * entity.setBar(record.getLong(1));
 * entity.setZzz(record.getString(2));
 * </pre>
 *
 * <h2><code>next</code> group</h2>
 * <p>
 * Move to the next column and get the value of the current column.<br>
 * Each column value can be retrieved only once.<br>
 * {@link #getCurrentColumnName()} and {@link #getCurrentColumnType()} can be used immediately after calling the next group method.
 * </p>
 *
 * <pre>
 * entity.setFoo(record.nextInt());
 * entity.setBar(record.nextLong());
 * entity.setZzz(record.nextString());
 * </pre>
 *
 * <p>
 * This class is linked with {@link TsurugiQueryResult} and the instance is shared among multiple records.<br>
 * Therefore, this instance must not be held by the user program for the purpose of holding the value of the record.<br>
 * Also, this class cannot be used after closing {@link TsurugiQueryResult}.
 * </p>
 */
@NotThreadSafe
public class TsurugiResultRecord implements TsurugiResultIndexRecord, TsurugiResultNameRecord, TsurugiResultNextRecord {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiResultRecord.class);

    private static IceaxeAmbiguousNamePolicy defaultAmbiguousNamePolicy = IceaxeAmbiguousNamePolicy.FIRST;

    /**
     * set default ambiguous name policy.
     *
     * @param policy default ambiguous name policy
     * @since 1.5.0
     */
    public static void setDefaultAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy policy) {
        defaultAmbiguousNamePolicy = Objects.requireNonNull(policy);
    }

    /**
     * get default ambiguous name policy.
     *
     * @return default ambiguous name policy
     * @since 1.5.0
     */
    public static IceaxeAmbiguousNamePolicy getDefaultAmbiguousNamePolicy() {
        return defaultAmbiguousNamePolicy;
    }

    private final TsurugiQueryResult<?> ownerResult;
    private final ResultSet lowResultSet;
    private final IceaxeConvertUtil convertUtil;
    private IceaxeAmbiguousNamePolicy ambiguousNamePolicy = null;

    private IceaxeResultNameList resultNameList = null;
    private List<TgDataType> typeList = null;

    private int currentColumnIndex;
    private Object[] values = null;
    private boolean isValuesAvailable;

    /**
     * Creates a new instance.
     *
     * @param result       query result
     * @param lowResultSet low ResultSet
     * @param convertUtil  convert type utility
     */
    protected TsurugiResultRecord(TsurugiQueryResult<?> result, ResultSet lowResultSet, IceaxeConvertUtil convertUtil) {
        this.ownerResult = result;
        this.lowResultSet = lowResultSet;
        this.convertUtil = convertUtil;
        reset();
    }

    void reset() {
        this.currentColumnIndex = -1;
        this.isValuesAvailable = false;
    }

    @Override
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    /**
     * get name list.
     *
     * @return list of column name
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     */
    public @Nonnull List<String> getNameList() throws IOException, InterruptedException, TsurugiTransactionException {
        return getResultNameList().getNameList();
    }

    /**
     * get name utility.
     *
     * @return name utility
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     * @since 1.5.0
     */
    @IceaxeInternal
    public IceaxeResultNameList getResultNameList() throws IOException, InterruptedException, TsurugiTransactionException {
        if (this.resultNameList == null) {
            var lowColumnList = TsurugiQueryResult.getLowColumnList(ownerResult, lowResultSet);
            this.resultNameList = IceaxeResultNameList.of(lowColumnList);
        }
        return this.resultNameList;
    }

    /**
     * get data type list.
     *
     * @return list of data type
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     */
    protected @Nonnull List<TgDataType> getTypeList() throws IOException, InterruptedException, TsurugiTransactionException {
        if (this.typeList == null) {
            var lowColumnList = TsurugiQueryResult.getLowColumnList(ownerResult, lowResultSet);
            var list = new ArrayList<TgDataType>(lowColumnList.size());
            for (var lowColumn : lowColumnList) {
                var type = TgDataType.of(lowColumn.getAtomType());
                list.add(type);
            }
            this.typeList = List.copyOf(list);
        }
        return this.typeList;
    }

    /*
     * current column
     */

    /**
     * move the current column to the next.
     *
     * @return true if the next column exists
     * @throws IOException                 if an I/O error occurs while retrieving the next column data
     * @throws InterruptedException        if interrupted while retrieving the next column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the next column data
     * @see #getCurrentColumnName()
     * @see #getCurrentColumnType()
     * @see #fetchCurrentColumnValue()
     */
    public boolean moveCurrentColumnNext() throws IOException, InterruptedException, TsurugiTransactionException {
        try {
            LOG.trace("nextLowColumn start");
            boolean exists = lowResultSet.nextColumn();
            if (LOG.isTraceEnabled()) {
                LOG.trace("nextLowColumn end. exists={}", exists);
            }
            if (exists) {
                this.currentColumnIndex++;
            }
            return exists;
        } catch (ServerException e) {
            throw ownerResult.fillToTsurugiException(new TsurugiTransactionException(e));
        }
    }

    @IceaxeInternal
    @Override
    public int getCurrentColumnIndex() {
        return this.currentColumnIndex;
    }

    /**
     * get current column name.
     *
     * @return column name
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     * @see #moveCurrentColumnNext()
     */
    public @Nonnull String getCurrentColumnName() throws IOException, InterruptedException, TsurugiTransactionException {
        return getNameList().get(currentColumnIndex);
    }

    /**
     * get current column data type.
     *
     * @return data type
     * @throws IOException                 if an I/O error occurs while retrieving metadata
     * @throws InterruptedException        if interrupted while retrieving metadata
     * @throws TsurugiTransactionException if server error occurs while retrieving metadata
     * @see #moveCurrentColumnNext()
     */
    public @Nonnull TgDataType getCurrentColumnType() throws IOException, InterruptedException, TsurugiTransactionException {
        return getType(currentColumnIndex);
    }

    /**
     * get current column value (take once).
     *
     * @return value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @see #moveCurrentColumnNext()
     */
    public @Nullable Object fetchCurrentColumnValue() throws IOException, InterruptedException, TsurugiTransactionException {
        if (lowResultSet.isNull()) {
            return null;
        }
        var lowType = getCurrentColumnType().getLowDataType();
        try {
            switch (lowType) {
            case BOOLEAN:
                return lowResultSet.fetchBooleanValue();
            case INT4:
                return lowResultSet.fetchInt4Value();
            case INT8:
                return lowResultSet.fetchInt8Value();
            case FLOAT4:
                return lowResultSet.fetchFloat4Value();
            case FLOAT8:
                return lowResultSet.fetchFloat8Value();
            case DECIMAL:
                return lowResultSet.fetchDecimalValue();
            case CHARACTER:
                return lowResultSet.fetchCharacterValue();
            case OCTET:
                return lowResultSet.fetchOctetValue();
            case BIT:
                return lowResultSet.fetchBitValue();
            case DATE:
                return lowResultSet.fetchDateValue();
            case TIME_OF_DAY:
                return lowResultSet.fetchTimeOfDayValue();
            case TIME_POINT:
                return lowResultSet.fetchTimePointValue();
            case DATETIME_INTERVAL:
                return lowResultSet.fetchDateTimeIntervalValue();
            case TIME_OF_DAY_WITH_TIME_ZONE:
                return lowResultSet.fetchTimeOfDayWithTimeZoneValue();
            case TIME_POINT_WITH_TIME_ZONE:
                return lowResultSet.fetchTimePointWithTimeZoneValue();
            default:
                throw new UnsupportedOperationException("unsupported type error. lowType=" + lowType);
            }
        } catch (ServerException e) {
            throw ownerResult.fillToTsurugiException(new TsurugiTransactionException(e));
        }
    }

    /*
     * get by index
     */

    @Override
    public @Nullable Object getValueOrNull(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        if (!isValuesAvailable) {
            if (this.values == null) {
                int size = getResultNameList().size();
                this.values = new Object[size];
            }

            readValues(this.values);

            this.isValuesAvailable = true;
        }
        return values[index];
    }

    void readValues(Object[] values) throws IOException, InterruptedException, TsurugiTransactionException {
        int i = 0;
        while (moveCurrentColumnNext()) {
            var value = fetchCurrentColumnValue();
            values[i++] = value;
        }
        if (i != values.length) {
            throw new IllegalStateException(String.format("column size unmatch. readColumn=%d, columnSize=%d", i, values.length));
        }
    }

    /**
     * get data type.
     *
     * @param index column index
     * @return data type
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public @Nonnull TgDataType getType(int index) throws IOException, InterruptedException, TsurugiTransactionException {
        return getTypeList().get(index);
    }

    /*
     * get by name
     */

    /**
     * set ambiguous name policy.
     *
     * @param policy ambiguous name policy
     * @since 1.5.0
     */
    public void setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy policy) {
        this.ambiguousNamePolicy = policy;
    }

    /**
     * get ambiguous name policy.
     *
     * @return ambiguous name policy
     * @since 1.5.0
     */
    public IceaxeAmbiguousNamePolicy getAmbiguousNamePolicy() {
        return this.ambiguousNamePolicy;
    }

    /**
     * get index.
     *
     * @param name column name
     * @return column index
     * @see #setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy)
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.5.0
     */
    public int getIndex(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        var policy = this.ambiguousNamePolicy;
        if (policy == null) {
            policy = defaultAmbiguousNamePolicy;
        }
        return getResultNameList().getIndex(name, policy);
    }

    /**
     * get index.
     *
     * @param name     column name
     * @param subIndex index for same name
     * @return column index
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     * @since 1.5.0
     */
    public int getIndex(String name, int subIndex) throws IOException, InterruptedException, TsurugiTransactionException {
        return getResultNameList().getIndex(name, subIndex);
    }

    @Override
    public @Nullable Object getValueOrNull(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        int index = getIndex(name);
        return getValueOrNull(index);
    }

    /**
     * get data type.
     *
     * @param name column name
     * @return data type
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public @Nonnull TgDataType getType(String name) throws IOException, InterruptedException, TsurugiTransactionException {
        int index = getIndex(name);
        return getType(index);
    }

    /*
     * next
     */

    /**
     * move next column.
     *
     * @throws IOException                 if an I/O error occurs while retrieving the next column data
     * @throws InterruptedException        if interrupted while retrieving the next column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the next column data
     * @throws IllegalStateException       if not found next column
     */
    public void nextColumn() throws IOException, InterruptedException, TsurugiTransactionException {
        boolean exists = moveCurrentColumnNext();
        if (!exists) {
            throw new IllegalStateException("not found next column");
        }
    }

    /**
     * get current column value and move next column.
     *
     * @return column value
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    @Override
    public @Nullable Object nextValueOrNull() throws IOException, InterruptedException, TsurugiTransactionException {
        nextColumn();
        return fetchCurrentColumnValue();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + lowResultSet + "}";
    }
}
