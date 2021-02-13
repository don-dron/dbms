package ru.bmstu.iu9.db.zvoa.dbms.utils;

import org.apache.log4j.PropertyConfigurator;
import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.JSQLInterpreter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.DBMSDataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.FileSystemManager;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.FileSystemManagerConfig;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.lsm.LsmStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.DBMSServer;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpRequestHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.io.http.HttpResponseHandler;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.query.DSQLQueryHandler;
import ru.bmstu.iu9.db.zvoa.dbms.execute.interpreter.storage.DataStorageException;
import ru.bmstu.iu9.db.zvoa.dbms.io.InputRequestModule;
import ru.bmstu.iu9.db.zvoa.dbms.io.OutputResponseModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryModule;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryRequestStorage;
import ru.bmstu.iu9.db.zvoa.dbms.query.QueryResponseStorage;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

public final class DBMSUtils {

    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        URL url = loader.getResource("log4j.properties");
        PropertyConfigurator.configure(url);
    }

    public static final DBMSConfig getDefaultConfig() {
        // TODO сделать нормально
        File file = new File("./");
        String path = file.getAbsolutePath();
        path = path.substring(0, path.length() - 1) + "data";

        return DBMSConfig.Builder.newBuilder()
                .setMountPath(path)
                .setPort(8890)
                .build();
    }

    public static final DBMS createDefaultDBMS() throws IOException, DataStorageException {
        return createDBMS(getDefaultConfig());
    }

    public static final DBMS createDBMS(DBMSConfig config) throws IOException, DataStorageException {
        DBMSServer httpServer = new DBMSServer(config.getPort());
        FileSystemManager fileSystemManager = new FileSystemManager(
                FileSystemManagerConfig.Builder.newBuilder()
                        .setStorageProperties(new StorageProperties("Root", config.getMountPath()))
                        .setStorageSupplier((properties) -> {
                            try {
                                return new LsmStorage(properties);
                            } catch (DataStorageException dataStorageException) {
                                dataStorageException.printStackTrace();
                                return null;
                            }
                        })
                        .build());

        DBMSDataStorage dataStorage = new DBMSDataStorage.Builder()
                .setFileSystemManager(fileSystemManager).build();

        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(config.getQueueSupplier().get(), config.getQueueSupplier().get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(config.getQueueSupplier().get(), config.getQueueSupplier().get());

        DBMS dbms = DBMS.Builder.newBuilder()
                .setAdditionalModules(Arrays.asList(httpServer, dataStorage))
                .setQueryModule(QueryModule.Builder.newBuilder()
                        .setQueryHandler(new DSQLQueryHandler(new JSQLInterpreter(dataStorage)))
                        .setQueryRequestStorage(queryRequestStorage)
                        .setQueryResponseStorage(queryResponseStorage)
                        .build())
                .setInputModule(new InputRequestModule(httpServer,
                        queryRequestStorage,
                        new HttpRequestHandler()))
                .setOutputModule(new OutputResponseModule(httpServer,
                        queryResponseStorage,
                        new HttpResponseHandler()))
                .build();
        return dbms;
    }
}
