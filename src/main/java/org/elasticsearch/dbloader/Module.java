package org.elasticsearch.dbloader;

import org.elasticsearch.common.inject.AbstractModule;

import java.io.IOException;

/**
 * Created by Ivan Zarudnyi on 10.10.2015.
 */
public class Module extends AbstractModule {
    @Override
    protected void configure() {
        bind(RestHandler.class).asEagerSingleton();
        try {
            ConfigProvider.init();
        } catch (IOException e) {
            throw new RuntimeException("DBLoader plugin: failed to load properties file",e);
        }
    }
}
