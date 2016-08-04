package io.subutai.plugin.cassandra.impl;


import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;


public class CassandraWebModule implements WebuiModule
{
    private static String NAME = "Cassandra";
    private static String IMG = "plugins/cassandra/cassandra.png";
    private static final String SIZE = "MEDIUM";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "cassandra37", 3 );
    }


    private WebuiModuleResourse cassandraResource;


    public void init()
    {
        cassandraResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.cassandra", "plugins/cassandra/cassandra.js",
                        "plugins/cassandra/controller.js", "plugins/cassandra/service.js",
                        "subutai-app/environment/service.js", "subutai-app/peerRegistration/service.js" );

        cassandraResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return cassandraResource.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), cassandraResource.getAngularjsList() );
    }
}
