package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

/**
 * Tsurugi Variable
 * 
 * @param <T> data type
 * @see TgVariableList#of(TgVariable...)
 */
public abstract class TgVariable<T> {

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<Boolean> ofBoolean(String name) {
        return new TgVariable<>(name, TgDataType.BOOLEAN) {

            @Override
            public TgParameter bind(Boolean value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<Integer> ofInt4(String name) {
        return new TgVariable<>(name, TgDataType.INT4) {

            @Override
            public TgParameter bind(Integer value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<Long> ofInt8(String name) {
        return new TgVariable<>(name, TgDataType.INT8) {

            @Override
            public TgParameter bind(Long value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<Float> ofFloat4(String name) {
        return new TgVariable<>(name, TgDataType.FLOAT4) {

            @Override
            public TgParameter bind(Float value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<Double> ofFloat8(String name) {
        return new TgVariable<>(name, TgDataType.FLOAT8) {

            @Override
            public TgParameter bind(Double value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<BigDecimal> ofDecimal(String name) {
        return new TgVariable<>(name, TgDataType.DECIMAL) {

            @Override
            public TgParameter bind(BigDecimal value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<String> ofCharacter(String name) {
        return new TgVariable<>(name, TgDataType.CHARACTER) {

            @Override
            public TgParameter bind(String value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<byte[]> ofBytes(String name) {
        return new TgVariable<>(name, TgDataType.BYTES) {

            @Override
            public TgParameter bind(byte[] value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<boolean[]> ofBits(String name) {
        return new TgVariable<>(name, TgDataType.BITS) {

            @Override
            public TgParameter bind(boolean[] value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<LocalDate> ofDate(String name) {
        return new TgVariable<>(name, TgDataType.DATE) {

            @Override
            public TgParameter bind(LocalDate value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<LocalTime> ofTime(String name) {
        return new TgVariable<>(name, TgDataType.TIME) {

            @Override
            public TgParameter bind(LocalTime value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<Instant> ofInstant(String name) {
        return new TgVariable<>(name, TgDataType.INSTANT) {

            @Override
            public TgParameter bind(Instant value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    /**
     * create Tsurugi Variable
     * 
     * @param name name
     * @return Tsurugi Variable
     */
    public static TgVariable<ZonedDateTime> ofZonedDateTime(String name) {
        return new TgVariable<>(name, TgDataType.INSTANT) {

            @Override
            public TgParameter bind(ZonedDateTime value) {
                return TgParameter.of(name(), value);
            }
        };
    }

    private final String name;
    private final TgDataType type;

    protected TgVariable(String name, TgDataType type) {
        this.name = name;
        this.type = type;
    }

    /**
     * get name
     * 
     * @return name
     */
    public String name() {
        return this.name;
    }

    /**
     * get name
     * 
     * @return name
     */
    public String sqlName() {
        return ":" + this.name;
    }

    /**
     * get type
     * 
     * @return type
     */
    public TgDataType type() {
        return this.type;
    }

    /**
     * bind value
     * 
     * @param value value
     * @return Tsurugi Parameter
     */
    public abstract TgParameter bind(T value);

    @Override
    public String toString() {
        return TgVariable.class.getSimpleName() + "{name=" + name + ", type=" + type + "}";
    }
}
