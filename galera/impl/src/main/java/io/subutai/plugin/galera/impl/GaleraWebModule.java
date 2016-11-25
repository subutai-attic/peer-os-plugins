package io.subutai.plugin.galera.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class GaleraWebModule implements WebuiModule
{
    private static String NAME = "Galera";
    private static String IMG = "plugins/galera/galera.png";
    private static final String SIZE = "LARGE";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "galera", 3 );
    }


    private WebuiModuleResourse galeraResource;


    public void init()
    {
        galeraResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.galera", "plugins/galera/galera.js",
                        "plugins/galera/controller.js", "plugins/galera/service.js",
                        "subutai-app/environment/service.js" );

        galeraResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return galeraResource.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), galeraResource.getAngularjsList() );
    }
}
