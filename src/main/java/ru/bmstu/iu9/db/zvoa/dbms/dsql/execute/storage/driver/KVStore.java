package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.shared.KVItem;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

public interface KVStore {

    String put(KVItem item) throws IOException;

    KVItem get(String key) throws IOException;

    Set<String> getAllKeys(Predicate<String> predicate) throws IOException;

    Set<KVItem> scan(String key) throws IOException;
}
