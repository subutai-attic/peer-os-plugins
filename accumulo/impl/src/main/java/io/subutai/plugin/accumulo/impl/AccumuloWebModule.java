package io.subutai.plugin.accumulo.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class AccumuloWebModule implements WebuiModule
{
    private static String NAME = "Accumulo";
    private static String IMG = "plugins/accumulo/accumulo.png";
    private static final String SIZE = "LARGE";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "hadoop", 3 );
    }


    private WebuiModuleResourse accumuloResource;


    public void init()
    {
        accumuloResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.accumulo", "plugins/accumulo/accumulo.js",
                        "plugins/accumulo/controller.js", "plugins/accumulo/service.js", "plugins/hadoop/service.js",
                        "subutai-app/environment/service.js" );

        accumuloResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return accumuloResource.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), accumuloResource.getAngularjsList() );
    }

}
