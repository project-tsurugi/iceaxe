/*
 * Copyright 2023-2025 Project Tsurugi.
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
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.result.TsurugiResultIndexEntity;
import com.tsurugidb.iceaxe.result.TsurugiResultNameEntity;
import com.tsurugidb.iceaxe.sql.result.IceaxeResultNameList.IceaxeAmbiguousNamePolicy;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi Result Entity.
 */
@ThreadSafe
public class TsurugiResultEntity implements TsurugiResultIndexEntity, TsurugiResultNameEntity {

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

    /**
     * create entity.
     *
     * @param record Tsurugi Result Record
     * @return entity
     * @throws IOException                 if an I/O error occurs while retrieving the column data
     * @throws InterruptedException        if interrupted while retrieving the column data
     * @throws TsurugiTransactionException if server error occurs while retrieving the column data
     */
    public static TsurugiResultEntity of(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        IceaxeResultNameList nameList = record.getResultNameList();

        var values = new Object[nameList.size()];
        record.readValues(values, true);

        var entity = new TsurugiResultEntity(nameList, values);
        entity.setConvertUtil(record.getConvertUtil());
        return entity;
    }

    private final IceaxeResultNameList resultNameList;
    private final Object[] values;
    private IceaxeAmbiguousNamePolicy ambiguousNamePolicy = null;
    private IceaxeConvertUtil convertUtil = IceaxeConvertUtil.INSTANCE;

    /**
     * Creates a new instance.
     *
     * @param nameList name utility
     * @param values   column values
     * @since 1.5.0
     */
    @IceaxeInternal
    protected TsurugiResultEntity(IceaxeResultNameList nameList, Object[] values) {
        this.resultNameList = nameList;
        this.values = values;
    }

    /**
     * set convert type utility.
     *
     * @param convertUtil convert type utility
     */
    public void setConvertUtil(IceaxeConvertUtil convertUtil) {
        this.convertUtil = convertUtil;
    }

    @Override
    public IceaxeConvertUtil getConvertUtil() {
        return this.convertUtil;
    }

    /**
     * get column name list.
     *
     * @return list of column name
     */
    public @Nonnull List<String> getNameList() {
        return this.resultNameList.getNameList();
    }

    /*
     * get by index
     */

    /**
     * get column name.
     *
     * @param index column index
     * @return column name
     * @throws IndexOutOfBoundsException if the index is out of range ({@code index < 0 || index >= size()})
     */
    public String getName(int index) {
        return getNameList().get(index);
    }

    @Override
    public @Nullable Object getValueOrNull(int index) {
        return values[index];
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
     * @since 1.5.0
     */
    public int getIndex(String name) {
        var policy = this.ambiguousNamePolicy;
        if (policy == null) {
            policy = defaultAmbiguousNamePolicy;
        }
        return resultNameList.getIndex(name, policy);
    }

    /**
     * get index.
     *
     * @param name     column name
     * @param subIndex index for same name
     * @return column index
     * @since 1.5.0
     */
    public int getIndex(String name, int subIndex) {
        return resultNameList.getIndex(name, subIndex);
    }

    @Override
    public @Nullable Object getValueOrNull(String name) {
        int index = getIndex(name);
        return getValueOrNull(index);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(256);
        sb.append(getClass().getSimpleName());
        sb.append('{');
        var nameList = getNameList();
        for (int i = 0; i < values.length; i++) {
            if (i != 0) {
                sb.append(", ");
            }

            String name = nameList.get(i);
            Object value = values[i];
            sb.append(name);
            sb.append('=');
            sb.append(value);
        }
        sb.append('}');
        return sb.toString();
    }
}
