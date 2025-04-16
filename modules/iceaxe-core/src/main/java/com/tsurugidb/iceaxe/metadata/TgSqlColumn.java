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

import javax.annotation.Nullable;

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
     * Get SQL type or AtomType name.
     *
     * @return SQL type, or AtomType name if the server does not support detail.
     */
    public String getSqlTypeOrAtomTypeName() {
        return findSqlType().orElse(lowColumn.getAtomType().name());
    }

    /**
     * Get SQL type.
     *
     * @return SQL type
     */
    public Optional<String> findSqlType() {
        String type = getSqlType();
        return Optional.ofNullable(type);
    }

    /**
     * Get SQL type.
     *
     * @return SQL type. {@code null} if the server does not support detail.
     */
    public @Nullable String getSqlType() {
        var atomType = lowColumn.getAtomType();
        switch (atomType) {
        case BOOLEAN:
            return getSqlTypeBoolean();
        case INT4:
            return getSqlTypeInt();
        case INT8:
            return getSqlTypeBigint();
        case FLOAT4:
            return getSqlTypeReal();
        case FLOAT8:
            return getSqlTypeDouble();
        case DECIMAL:
            return getSqlTypeDecimal();
        case CHARACTER:
            return getSqlTypeVarLength("CHAR");
        case OCTET:
            return getSqlTypeVarLength("BINARY");
        case BIT:
            return getSqlTypeBit();
        case DATE:
            return getSqlTypeDate();
        case TIME_OF_DAY:
            return getSqlTypeTime();
        case TIME_POINT:
            return getSqlTypeTimestamp();
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return getSqlTypeTimeWithTimeZone();
        case TIME_POINT_WITH_TIME_ZONE:
            return getSqlTypeTimestampWithTimeZone();
        case BLOB:
            return getSqlTypeBlob();
        case CLOB:
            return getSqlTypeClob();
        default:
            return null;
        }
    }

    /**
     * Get SQL type (BOOLEAN).
     *
     * @return SQL type
     */
    protected String getSqlTypeBoolean() {
        return "BOOLEAN";
    }

    /**
     * Get SQL type (INT).
     *
     * @return SQL type
     */
    protected String getSqlTypeInt() {
        return "INT";
    }

    /**
     * Get SQL type (BIGINT).
     *
     * @return SQL type
     */
    protected String getSqlTypeBigint() {
        return "BIGINT";
    }

    /**
     * Get SQL type (REAL).
     *
     * @return SQL type
     */
    protected String getSqlTypeReal() {
        return "REAL";
    }

    /**
     * Get SQL type (REAL).
     *
     * @return SQL type
     */
    protected String getSqlTypeDouble() {
        return "DOUBLE";
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
     * @param baseName base name
     * @return SQL type
     */
    protected String getSqlTypeVarLength(String baseName) {
        var varying = findVarying();
        if (varying.isEmpty()) {
            return null;
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
     * Get SQL type (BIT).
     *
     * @return SQL type
     */
    protected String getSqlTypeBit() {
        return "BIT";
    }

    /**
     * Get SQL type (DATE).
     *
     * @return SQL type
     */
    protected String getSqlTypeDate() {
        return "DATE";
    }

    /**
     * Get SQL type (TIME).
     *
     * @return SQL type
     */
    protected String getSqlTypeTime() {
        return "TIME";
    }

    /**
     * Get SQL type (TIMESTAMP).
     *
     * @return SQL type
     */
    protected String getSqlTypeTimestamp() {
        return "TIMESTAMP";
    }

    /**
     * Get SQL type (TIME WITH TIME ZONE).
     *
     * @return SQL type
     */
    protected String getSqlTypeTimeWithTimeZone() {
        return "TIME WITH TIME ZONE";
    }

    /**
     * Get SQL type (TIMESTAMP WITH TIME ZONE).
     *
     * @return SQL type
     */
    protected String getSqlTypeTimestampWithTimeZone() {
        return "TIMESTAMP WITH TIME ZONE";
    }

    /**
     * Get SQL type (BLOB).
     *
     * @return SQL type
     */
    protected String getSqlTypeBlob() {
        return "BLOB";
    }

    /**
     * Get SQL type (CLOB).
     *
     * @return SQL type
     */
    protected String getSqlTypeClob() {
        return "CLOB";
    }

    /**
     * Get description.
     *
     * @return description
     */
    public @Nullable String getDescription() {
        var c = lowColumn.getDescriptionOptCase();
        if (c == null) {
            return null;
        }
        switch (c) {
        case DESCRIPTION:
            return lowColumn.getDescription();
        case DESCRIPTIONOPT_NOT_SET:
        default:
            return null;
        }
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
