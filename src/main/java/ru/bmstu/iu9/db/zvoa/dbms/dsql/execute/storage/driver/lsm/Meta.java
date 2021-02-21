package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.lsm;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Key;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.Value;

import java.util.Arrays;

/**
 * Meta information about SSTable.
 *
 * @param <K> - type of keys
 * @param <V> - type of values
 *
 * @author don-dron Zvorygin Andrey BMSTU IU-9
 */
public class Meta<K extends Key, V extends Value> {
    private final K[] keys;

    /**
     * Constructor.
     *
     * @param keys - key dump for SSTable
     */
    public Meta(K[] keys) {
        this.keys = keys;
    }

    /**
     * Get keys.
     *
     * @return SSTable keys
     */
    public K[] getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "Meta{" +
                "keys=" + Arrays.toString(keys) +
                "}";
    }
}
