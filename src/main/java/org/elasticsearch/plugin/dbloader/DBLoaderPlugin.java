package org.elasticsearch.plugin.dbloader;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.dbloader.DBLoaderModule;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class DBLoaderPlugin extends AbstractPlugin {
     public String name() {
        return "dbloader";
    }

     public String description() {
        return "Example Plugin Description";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = Lists.newArrayList();
        modules.add(DBLoaderModule.class);
        return modules;
    }
}