package org.elasticsearch.plugin.dbloader;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.dbloader.Module;
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
    public Collection<Class<? extends org.elasticsearch.common.inject.Module>> modules() {
        Collection<Class<? extends org.elasticsearch.common.inject.Module>> modules = Lists.newArrayList();
        modules.add(Module.class);
        return modules;
    }
}