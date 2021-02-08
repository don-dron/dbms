package ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver;

import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.lsm.LSMCache;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.lsm.LSMFile;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.lsm.LSMFlusher;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.lsm.LSMLog;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.shared.Constants;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.storage.driver.shared.KVItem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LSMStore implements KVStore {

    private LSMLog lsmLog;
    private LSMCache lsmCache;
    private final Path lsmFileDir;
    private final Path root;

    public LSMStore(Path dataDir) throws IOException {
        if (!dataDir.toFile().exists()) {
            dataDir.toFile().mkdir();
        }

        root = dataDir;

        Path lsmLogFileDir = Paths.get(dataDir.toString(), "log");
        this.lsmFileDir = Paths.get(dataDir.toString(), "data");

        if (!lsmFileDir.toFile().exists()) {
            lsmFileDir.toFile().mkdir();
        }

        this.lsmLog = new LSMLog(lsmLogFileDir);
        TreeMap<String, KVItem> log = lsmLog.readAllSinceFlush();

        this.lsmCache = new LSMCache();

        log.forEach((s, i) -> lsmCache.put(i));

        LSMFlusher lsmFlusher = new LSMFlusher(lsmCache, lsmFileDir, lsmLog);
        lsmFlusher.start();
    }

    private List<LSMFile> listLSMFiles(Path lsmFileDir) throws IOException {
        List<LSMFile> lsmFiles = new ArrayList<>();
        try (Stream<Path> paths = Files.list(lsmFileDir)) {
            List<Path> files = paths.collect(Collectors.toList());
            for (Path f : files) {
                lsmFiles.add(new LSMFile(f.getParent(), f.getFileName().toString()));
            }
        }
        return lsmFiles;
    }

    public Path getRoot() {
        return root;
    }

    @Override
    public String put(KVItem item) throws IOException {
        if (item.getTimestamp() == 0) {
            item.setTimestamp(Instant.now().toEpochMilli());
        }
        String result = "success";
        if (get(item.getKey()) != null) {
            result = "update";
        }
        lsmLog.append(item);
        lsmCache.put(item);
        return result;
    }

    private static class Lookup {

        private final LSMFile lsmFile;
        private final long position;

        private Lookup(LSMFile lsmFile, long position) {
            this.lsmFile = lsmFile;
            this.position = position;
        }
    }

    @Override
    public KVItem get(String key) throws IOException {

        KVItem cachedItem = lsmCache.get(key);
        if (cachedItem != null) {
            if (!cachedItem.getValue().equals(Constants.DELETE_MARKER)) {
                return cachedItem;
            }
        }

        ArrayList<Lookup> lookUps = new ArrayList<>();
        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);

        for (LSMFile f : lsmFiles) {
            Long position = f.readIndex().get(key);
            if (position != null) {
                lookUps.add(new Lookup(f, position));
            } else {
                f.close();
            }
        }

        TreeMap<Long, KVItem> items = new TreeMap<>();
        for (Lookup l : lookUps) {
            KVItem kvItem = l.lsmFile.readValue(l.position);
            if (kvItem != null) {
                items.put(kvItem.getTimestamp(), kvItem);
            }
            l.lsmFile.close();
        }

        if (items.size() > 0) {
            KVItem result = items.lastEntry().getValue();
            if (!result.getValue().equals(Constants.DELETE_MARKER)) {
                return result;
            }
        }

        return null;
    }

    @Override
    public Set<String> getAllKeys(Predicate<String> predicate) throws IOException {

        TreeMap<String, KVItem> cacheSnapshot = lsmCache.getShallowLsmCopy();

        Set<String> matchingKeys = cacheSnapshot.keySet()
                .stream()
                .filter(predicate)
                .collect(Collectors.toSet());

        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);

        for (LSMFile f : lsmFiles) {
            TreeMap<String, Long> node = f.readIndex();
            matchingKeys.addAll(
                    node.keySet()
                            .stream()
                            .filter(predicate)
                            .collect(Collectors.toSet())
            );
            f.close();
        }

        return matchingKeys;
    }

    @Override
    public Set<KVItem> scan(String key) throws IOException {
        Set<KVItem> cachedSet = lsmCache.scan(key);
        Set<KVItem> totalSet = new HashSet<>(cachedSet);
        Set<KVItem> lsmSet = new HashSet<>();
        List<LSMFile> lsmFiles = listLSMFiles(lsmFileDir);
        Set<String> totalKeySet = new HashSet<>();
        Set<KVItem> matchingSet = new HashSet<>();

        for (LSMFile f : lsmFiles) {
            totalKeySet.addAll(f.readIndex().keySet());
        }
        ArrayList<Lookup> lookUp = new ArrayList<>();
        for (String k : totalKeySet) {
            lookUp.clear();
            for (LSMFile f : lsmFiles) {
                Long position = f.readIndex().get(k);
                if (position != null) {
                    lookUp.add(new Lookup(f, position));
                } else {
                    f.close();
                }
            }
            TreeMap<Long, KVItem> items = new TreeMap<>();
            for (Lookup l : lookUp) {
                KVItem kvItem = l.lsmFile.readValue(l.position);
                if (kvItem != null) {
                    items.put(kvItem.getTimestamp(), kvItem);
                }
                l.lsmFile.close();
            }
            if (items.size() > 0) {
                KVItem lastEntry = items.lastEntry().getValue();
                if (!lastEntry.getValue().equals(Constants.DELETE_MARKER)) {
                    lsmSet.add(lastEntry);
                }
            }
        }
        totalSet.addAll(lsmSet);

        for (KVItem item : totalSet) {
            if (item.getKey().contains(key)) {
                matchingSet.add(item);
            }
        }
        return matchingSet;
    }
}
