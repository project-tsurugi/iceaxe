package com.tsurugidb.iceaxe.statement;

/**
 * Tsurugi Variable
 * 
 * @param <T> data type
 * @see TgVariableList#of(TgVariable...)
 */
public abstract class TgVariable<T> {

    /**
     * create Tsurugi Variable(int)
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
     * create Tsurugi Variable(long)
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

    // TODO float, double

    /**
     * create Tsurugi Variable(String)
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
        return getClass().getSimpleName() + "{name=" + name + ", type=" + type + "}";
    }
}