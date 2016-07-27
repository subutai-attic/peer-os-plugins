package io.subutai.plugin.storm.impl;


import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;


public class StormWebModule implements WebuiModule
{
    private static String NAME = "Storm";
    private static String IMG = "plugins/storm/storm.png";
    private static final String SIZE = "SMALL";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "storm", 3 );
    }


    private WebuiModuleResourse sparkResource;


    public void init()
    {
        sparkResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.storm", "plugins/storm/storm.js",
                        "plugins/storm/controller.js", "plugins/storm/service.js",
                        "subutai-app/environment/service.js" );

        sparkResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return sparkResource.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), sparkResource.getAngularjsList() );
    }
}
