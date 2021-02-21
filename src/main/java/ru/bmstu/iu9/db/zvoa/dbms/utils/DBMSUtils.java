/*
 * Copyright (c) 2021 Zvorygin Andrey BMSTU IU-9 https://github.com/don-dron
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
 * OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT
 * OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package ru.bmstu.iu9.db.zvoa.dbms.utils;

import org.apache.log4j.PropertyConfigurator;
import ru.bmstu.iu9.db.zvoa.dbms.DBMS;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.interpreter.JSQLInterpreter;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.DBMSDataStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.FileSystemManager;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.FileSystemManagerConfig;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.StorageProperties;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.lsm.LSMStorage;
import ru.bmstu.iu9.db.zvoa.dbms.dsql.execute.storage.driver.converter.RootConverter;
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

    public static DBMSConfig getDefaultConfig() {
        // TODO сделать нормально
        File file = new File("./");
        String path = file.getAbsolutePath();
        path = path.substring(0, path.length() - 1) + "data";

        return DBMSConfig.Builder.newBuilder()
                .setMountPath(path)
                .setPort(8890)
                .build();
    }

    public static DBMS createDefaultDBMS() throws IOException, DataStorageException {
        return createDBMS(getDefaultConfig());
    }

    public static DBMS createDBMS(DBMSConfig config) throws IOException, DataStorageException {
        DBMSServer httpServer = new DBMSServer(config.getPort());
        FileSystemManager fileSystemManager = new FileSystemManager(
                FileSystemManagerConfig.Builder.newBuilder()
                        .setStorageProperties(new StorageProperties(new RootConverter(), "Root", config.getMountPath()))
                        .setStorageSupplier(LSMStorage::new)
                        .build());

        DBMSDataStorage dataStorage = new DBMSDataStorage.Builder()
                .setFileSystemManager(fileSystemManager).build();

        QueryRequestStorage queryRequestStorage =
                new QueryRequestStorage(config.getQueueSupplier().get(), config.getQueueSupplier().get());
        QueryResponseStorage queryResponseStorage =
                new QueryResponseStorage(config.getQueueSupplier().get(), config.getQueueSupplier().get());

        return DBMS.Builder.newBuilder()
                .setAdditionalModules(Arrays.asList(httpServer, dataStorage))
                .setQueryModuleBuilder(QueryModule.Builder.newBuilder()
                        .setQueryHandler(new DSQLQueryHandler(new JSQLInterpreter(dataStorage)))
                        .setQueryRequestStorage(queryRequestStorage)
                        .setQueryResponseStorage(queryResponseStorage))
                .setInputModule(new InputRequestModule(httpServer,
                        queryRequestStorage,
                        new HttpRequestHandler()))
                .setOutputModule(new OutputResponseModule(httpServer,
                        queryResponseStorage,
                        new HttpResponseHandler()))
                .build();
    }
}
