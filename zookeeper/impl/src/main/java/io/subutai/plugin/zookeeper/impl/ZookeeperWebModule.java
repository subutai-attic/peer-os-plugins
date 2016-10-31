package io.subutai.plugin.zookeeper.impl;


import com.google.gson.Gson;

import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;


public class ZookeeperWebModule implements WebuiModule
{
    private static String NAME = "Zookeeper";
    private static String IMG = "plugins/zookeeper/zookeeper.png";
    private static final String SIZE = "HUGE";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( ZookeeperClusterConfig.TEMPLATE_NAME, 3 );
    }


    private WebuiModuleResourse zooResource;


    public void init()
    {
        zooResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.zookeeper", "plugins/zookeeper/zookeeper.js",
                        "plugins/zookeeper/controller.js", "plugins/zookeeper/service.js", "plugins/hadoop/service.js",
                        "subutai-app/environment/service.js" );

        zooResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return zooResource.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), zooResource.getAngularjsList() );
    }
}
