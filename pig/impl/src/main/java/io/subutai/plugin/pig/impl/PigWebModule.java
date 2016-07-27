package io.subutai.plugin.pig.impl;


import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;


public class PigWebModule implements WebuiModule
{
    private static String NAME = "Pig";
    private static String IMG = "plugins/pig/pig.png";
    private static final String SIZE = "SMALL";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "hadoop", 3 );
    }


    private WebuiModuleResourse pigResource;


    public void init()
    {
        pigResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.pig", "plugins/pig/pig.js", "plugins/pig/controller.js",
                        "plugins/pig/service.js", "plugins/hadoop/service.js", "subutai-app/environment/service.js" );

        pigResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return pigResource.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), pigResource.getAngularjsList() );
    }
}
