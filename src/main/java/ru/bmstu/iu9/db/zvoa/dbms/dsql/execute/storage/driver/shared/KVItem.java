package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.shared;

import java.util.Base64;

public class KVItem {
    private final String key;
    private String value = null;
    private static final int KEY_MAX_BYTES = 20;
    private static final int VAL_MAX_BYTES = 120000;
    private long timestamp;

    public KVItem(String key) {
        this.key = key;
    }

    public KVItem(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public KVItem(String key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public boolean isValid() {
        if (this.key == null || this.key.isEmpty()) {
            return false;
        }
        if (this.key.getBytes().length > KEY_MAX_BYTES) {
            return false;
        }
        return this.value == null || this.value.getBytes().length <= VAL_MAX_BYTES;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean hasValue() {
        return (value != null);
    }

    public String getValueAs64() {
        if (hasValue()) {
            return Base64.getEncoder().encodeToString(value.getBytes());
        }
        return null;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setValueFrom64(String rawInput) {
        if (rawInput != null) {
            this.value = new String(Base64.getDecoder().decode(rawInput.getBytes()));
        } else {
            this.value = null;
        }
    }

    @Override
    public String toString() {
        String val = hasValue() ? " " + this.value : "";
        return this.key + val;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
