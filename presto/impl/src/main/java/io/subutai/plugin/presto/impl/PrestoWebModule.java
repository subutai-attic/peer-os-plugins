package io.subutai.plugin.presto.impl;


import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;


public class PrestoWebModule implements WebuiModule
{
    private static String NAME = "Presto";
    private static String IMG = "plugins/presto/presto.png";
    private static final String SIZE = "SMALL";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "hadoop", 3 );
    }


    private WebuiModuleResourse prestoResource;


    public void init()
    {
        prestoResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.presto", "plugins/presto/presto.js",
                        "plugins/presto/controller.js", "plugins/presto/service.js", "plugins/hadoop/service.js",
                        "subutai-app/environment/service.js" );

        prestoResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return prestoResource.getAngularjsList();
    }


    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public String getModuleInfo()
    {
        return String
                .format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"size\" : \"%s\", \"requirement\" : %s}", IMG, NAME,
                        SIZE, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString() );
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), prestoResource.getAngularjsList() );
    }
}
