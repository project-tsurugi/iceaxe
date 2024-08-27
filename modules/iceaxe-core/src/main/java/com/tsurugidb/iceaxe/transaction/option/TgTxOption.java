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

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option.
 */
public interface TgTxOption extends Cloneable {

    /**
     * create transaction option for short transaction.
     *
     * @return transaction option
     */
    public static TgTxOptionOcc ofOCC() {
        return new TgTxOptionOcc();
    }

    /**
     * create transaction option for short transaction.
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionOcc ofOCC(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return new TgTxOptionOcc().fillFrom(txOption);
    }

    /**
     * create transaction option for long transaction.
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(String... writePreserveTableNames) {
        return new TgTxOptionLtx().addWritePreserve(writePreserveTableNames);
    }

    /**
     * create transaction option for long transaction.
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(Collection<String> writePreserveTableNames) {
        return new TgTxOptionLtx().addWritePreserve(writePreserveTableNames);
    }

    /**
     * create transaction option for long transaction.
     *
     * @param writePreserveTableNames table name to Write Preserve
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(Stream<String> writePreserveTableNames) {
        return new TgTxOptionLtx().addWritePreserve(writePreserveTableNames);
    }

    /**
     * create transaction option for long transaction.
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionLtx ofLTX(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return new TgTxOptionLtx().fillFrom(txOption);
    }

    /**
     * create transaction option for read only transaction.
     *
     * @return transaction option
     */
    public static TgTxOptionRtx ofRTX() {
        return new TgTxOptionRtx();
    }

    /**
     * create transaction option for read only transaction.
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionRtx ofRTX(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return new TgTxOptionRtx().fillFrom(txOption);
    }

    /**
     * create transaction option for DDL(LTX).
     *
     * @return transaction option
     */
    public static TgTxOptionLtx ofDDL() {
        return new TgTxOptionLtx().includeDdl(true);
    }

    /**
     * create transaction option for DDL(LTX).
     *
     * @param txOption source transaction option
     * @return transaction option
     */
    public static TgTxOptionLtx ofDDL(@Nonnull TgTxOption txOption) {
        Objects.requireNonNull(txOption);
        return ofLTX(txOption).includeDdl(true);
    }

    /**
     * get transaction type name.
     *
     * @return transaction type
     */
    public String typeName();

    /**
     * get transaction type.
     *
     * @return transaction type
     */
    public TransactionType type();

    /**
     * check transaction type is OCC.
     *
     * @return {@code true} if OCC
     */
    public default boolean isOCC() {
        return type() == TransactionType.SHORT;
    }

    /**
     * check transaction type is LTX.
     *
     * @return {@code true} if LTX
     */
    public default boolean isLTX() {
        return type() == TransactionType.LONG;
    }

    /**
     * check transaction type is RTX.
     *
     * @return {@code true} if RTX
     */
    public default boolean isRTX() {
        return type() == TransactionType.READ_ONLY;
    }

    /**
     * set label.
     *
     * @param label label
     * @return this
     */
    public TgTxOption label(String label);

    /**
     * get label.
     *
     * @return label
     */
    public String label();

    /**
     * clone transaction option.
     *
     * @return new transaction option
     */
    public TgTxOption clone();

    /**
     * clone transaction option.
     *
     * @param label label
     * @return new transaction option
     */
    public TgTxOption clone(String label);

    /**
     * Casts this to the class represented by transaction option Class object.
     *
     * @param <T>           transaction option type
     * @param txOptionClass transaction option class
     * @return this after casting
     * @throws ClassCastException if this is not assignable to the type T
     */
    public default <T extends TgTxOption> T as(Class<T> txOptionClass) throws ClassCastException {
        return txOptionClass.cast(this);
    }

    /**
     * Casts this to OCC option class.
     *
     * @return this after casting
     * @throws ClassCastException if this is not assignable to OCC option
     * @see #isOCC()
     */
    public default TgTxOptionOcc asOccOption() throws ClassCastException {
        return (TgTxOptionOcc) this;
    }

    /**
     * Casts this to LTX option class.
     *
     * @return this after casting
     * @throws ClassCastException if this is not assignable to LTX option
     * @see #isLTX()
     */
    public default TgTxOptionLtx asLtxOption() throws ClassCastException {
        return (TgTxOptionLtx) this;
    }

    /**
     * Casts this to RTX option class.
     *
     * @return this after casting
     * @throws ClassCastException if this is not assignable to RTX option
     * @see #isRTX()
     */
    public default TgTxOptionRtx asRtxOption() throws ClassCastException {
        return (TgTxOptionRtx) this;
    }

    /**
     * convert to {@link TransactionOption}.
     *
     * @return transaction option
     */
    @IceaxeInternal
    public TransactionOption toLowTransactionOption();
}
