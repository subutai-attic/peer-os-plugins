package io.subutai.plugin.mahout.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class MahoutWebModule implements WebuiModule
{
    private WebuiModuleResourse mahoutResource;
    private static String NAME = "Mahout";
    private static String IMG = "plugins/mahout/mahout.png";
    private static final String SIZE = "SMALL";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "hadoop", 3 );
    }

    public void init()
    {
        this.mahoutResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.mahout", "plugins/mahout/mahout.js",
                        "plugins/mahout/controller.js", "plugins/mahout/service.js", "plugins/hadoop/service.js",
                        "subutai-app/environment/service.js" );

        this.mahoutResource.addDependency( angularjsDependency );
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
        return this.mahoutResource.getAngularjsList();
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.mahoutResource.getAngularjsList() );
    }
}
