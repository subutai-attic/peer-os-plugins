package io.subutai.plugin.hbase.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class HBaseWebModule implements WebuiModule
{
    private WebuiModuleResourse hbaseResource;
    private static String NAME = "HBase";
    private static String IMG = "plugins/hbase/hbase.png";
    private static final String SIZE = "SMALL";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "hadoop", 3 );
    }

    public void init()
    {
        this.hbaseResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.hbase", "plugins/hbase/hbase.js",
                        "plugins/hbase/controller.js", "plugins/hbase/service.js", "plugins/hadoop/service.js",
                        "subutai-app/environment/service.js" );

        this.hbaseResource.addDependency( angularjsDependency );
    }


    @Override
    public String getModuleInfo()
    {
        return String
                .format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"size\" : \"%s\", \"requirement\" : %s}", IMG, NAME,
                        SIZE, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString() );
    }


    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public String getAngularState()
    {
        return this.hbaseResource.getAngularjsList();
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.hbaseResource.getAngularjsList() );
    }
}
