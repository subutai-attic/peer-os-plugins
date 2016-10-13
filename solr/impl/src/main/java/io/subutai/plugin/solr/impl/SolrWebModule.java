package io.subutai.plugin.solr.impl;


import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;


public class SolrWebModule implements WebuiModule
{
    private static String NAME = "Solr";
    private static String IMG = "plugins/solr/solr.png";
    private static final String SIZE = "SMALL";


    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "solr62", 3 );
    }


    private WebuiModuleResourse solr;


    public void init()
    {
        solr = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.solr", "plugins/solr/solr.js", "plugins/solr/controller.js",
                        "plugins/solr/service.js", "subutai-app/environment/service.js" );

        solr.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return solr.getAngularjsList();
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
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), solr.getAngularjsList() );
    }
}
