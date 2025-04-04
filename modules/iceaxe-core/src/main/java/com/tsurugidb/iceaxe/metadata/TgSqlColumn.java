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
package com.tsurugidb.iceaxe.metadata;

import java.util.Optional;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlCommon.Column;

/**
 * Tsurugi column.
 *
 * @since X.X.X
 */
public class TgSqlColumn {

    /** low column */
    protected final Column lowColumn;

    /**
     * Creates a new instance.
     *
     * @param lowColumn low column
     */
    public TgSqlColumn(Column lowColumn) {
        this.lowColumn = lowColumn;
    }

    /**
     * Get column name.
     *
     * @return column name
     */
    public String getName() {
        return lowColumn.getName();
    }

    /**
     * Get data type.
     *
     * @return data type
     */
    public TgDataType getDataType() {
        return TgDataType.of(lowColumn.getAtomType());
    }

    /**
     * Get length for data types.
     *
     * @return length
     */
    public Optional<ArbitraryInt> findLength() {
        var c = lowColumn.getLengthOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case LENGTH:
            return Optional.of(ArbitraryInt.of(lowColumn.getLength()));
        case ARBITRARY_LENGTH:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case LENGTHOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Get precision for decimal types.
     *
     * @return precision
     */
    public Optional<ArbitraryInt> findPrecision() {
        var c = lowColumn.getPrecisionOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case PRECISION:
            return Optional.of(ArbitraryInt.of(lowColumn.getPrecision()));
        case ARBITRARY_PRECISION:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case PRECISIONOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Get scale for decimal types.
     *
     * @return scale
     */
    public Optional<ArbitraryInt> findScale() {
        var c = lowColumn.getScaleOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case SCALE:
            return Optional.of(ArbitraryInt.of(lowColumn.getScale()));
        case ARBITRARY_SCALE:
            return Optional.of(ArbitraryInt.ofArbitrary());
        case SCALEOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Whether the column type is nullable.
     *
     * @return nullable
     */
    public Optional<Boolean> findNullable() {
        var c = lowColumn.getNullableOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case NULLABLE:
            return Optional.of(lowColumn.getNullable());
        case NULLABLEOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Whether the column type is varying.
     *
     * @return varying
     */
    public Optional<Boolean> findVarying() {
        var c = lowColumn.getVaryingOptCase();
        if (c == null) {
            return Optional.empty();
        }
        switch (c) {
        case VARYING:
            return Optional.of(lowColumn.getVarying());
        case VARYINGOPT_NOT_SET:
        default:
            return Optional.empty();
        }
    }

    /**
     * Get SQL type.
     * <p>
     * Returns the name of AtomType if the server does not support detail. (e.g. DECIMAL, CHARACTER, OCTET)
     * </p>
     *
     * @return SQL type
     */
    public String getSqlType() {
        var atomType = lowColumn.getAtomType();
        switch (atomType) {
        case INT4:
            return "INT";
        case INT8:
            return "BIGINT";
        case FLOAT4:
            return "REAL";
        case FLOAT8:
            return "DOUBLE";
        case DECIMAL:
            return getSqlTypeDecimal();
        case CHARACTER:
            return getSqlTypeVarLength("CHARACTER", "CHAR");
        case OCTET:
            return getSqlTypeVarLength("BINARY", "BINARY");
        case DATE:
            return "DATE";
        case TIME_OF_DAY:
            return "TIME";
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return "TIME WITH TIME ZONE";
        case TIME_POINT:
            return "TIMESTAMP";
        case TIME_POINT_WITH_TIME_ZONE:
            return "TIMESTAMP WITH TIME ZONE";
        default:
            return atomType.toString();
        }
    }

    /**
     * Get SQL type (DECIMAL).
     *
     * @return SQL type
     */
    protected String getSqlTypeDecimal() {
        var sb = new StringBuilder("DECIMAL");
        var precision = findPrecision();
        if (precision.isPresent()) {
            sb.append('(');
            sb.append(precision.get());

            var scale = findScale();
            if (scale.isPresent()) {
                sb.append(", ");
                sb.append(scale.get());
            }
            sb.append(')');
        }
        return sb.toString();
    }

    /**
     * Get SQL type (VAR(length)).
     *
     * @param defaultName default name
     * @param baseName    base name
     * @return SQL type
     */
    protected String getSqlTypeVarLength(String defaultName, String baseName) {
        var varying = findVarying();
        if (varying.isEmpty()) {
            return defaultName;
        }

        var sb = new StringBuilder();
        if (varying.get()) {
            sb.append("VAR");
        }
        sb.append(baseName);

        var length = findLength();
        if (length.isPresent()) {
            sb.append('(');
            sb.append(length.get());
            sb.append(')');
        }
        return sb.toString();
    }

    /**
     * Get low column.
     *
     * @return low column
     */
    @IceaxeInternal
    public Column getLowColumn() {
        return this.lowColumn;
    }
}
