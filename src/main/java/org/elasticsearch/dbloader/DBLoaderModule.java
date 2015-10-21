package org.elasticsearch.dbloader;

import org.elasticsearch.common.inject.AbstractModule;

/**
 * Created by Ivan Zarudnyi on 10.10.2015.
 */
public class DBLoaderModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(DBLoaderRestHandler.class).asEagerSingleton();
    }
}
