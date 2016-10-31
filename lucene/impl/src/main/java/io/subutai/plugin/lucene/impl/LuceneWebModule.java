package io.subutai.plugin.lucene.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class LuceneWebModule implements WebuiModule
{
    private WebuiModuleResourse luceneResource;
    private static String NAME = "Lucene";
    private static String IMG = "plugins/lucene/lucene.png";
    private static final String SIZE = "LARGE";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "hadoop", 3 );
    }

    public void init()
    {
        this.luceneResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.lucene", "plugins/lucene/lucene.js",
                        "plugins/lucene/controller.js", "plugins/lucene/service.js", "plugins/hadoop/service.js",
                        "subutai-app/environment/service.js" );

        this.luceneResource.addDependency( angularjsDependency );
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
        return this.luceneResource.getAngularjsList();
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.luceneResource.getAngularjsList() );
    }
}
