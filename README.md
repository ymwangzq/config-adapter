# config-adapter

This is a powerful dynamic config util, almost satisfied any requirement.

## Usage

dependency

```xml

<dependency>
    <groupId>io.github.ymwangzq</groupId>
    <artifactId>config-adapter-core</artifactId>
    <version>1.1.0</version>
</dependency>
```

Basic usage:

```java
package com.github.ymwangzq.config.adapter.core;

import java.util.Objects;

import javax.annotation.Nullable;

import com.github.ymwangzq.config.adapter.facade.ConfigAdapter;
import com.github.ymwangzq.config.adapter.facade.ConfigAdapterMapper;

public class ConfigAdapterTest {
    /**
     * A function to get current latest config value
     */
    private static String getCurrentConfigValue() {
        // get latest config value, maybe from zk / apollo or any other config center.
        return ...;
    }

    /**
     * some resource, for example: DataSource connection
     */
    public static class Connection implements AutoCloseable {

        private final String connectionString;

        private Connection(String connectionString) {
            this.connectionString = connectionString;
        }

        public void connect() {
            // ...
        }

        /**
         * some method provided by this connection
         */
        public void sendRequest(...) {
            // ...
        }

        public void close() throws Exception {
            // ...
        }
    }

    // this is a dynamic config
    private static final ConfigAdapter<String> CONFIG =
            SupplierConfigAdapter.createConfigAdapter(ConfigAdapterTest::getCurrentConfigValue);
    // and this is a dynamic resource
    private static final ConfigAdapter<Connection> DYNAMIC_CONNECTION =
            CONFIG.map(new ConfigAdapterMapper<String, Connection>() {
                @Override
                public Connection map(@Nullable String s) {
                    Objects.requireNonNull(s);
                    return new Connection(s);
                }

                @Override
                public void cleanUp(@Nullable Connection connection) {
                    if (connection != null) {
                        try {
                            connection.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });

    public void sendRequest(...) {
        // always use the latest config
        DYNAMIC_CONNECTION.get().sendRequest(...);
    }
}
```

More powerful utils methods, see `com.github.ymwangzq.config.adapter.core.ConfigAdapters`