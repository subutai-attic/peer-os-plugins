package io.subutai.plugin.flume.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class FlumeWebModule implements WebuiModule
{
    private WebuiModuleResourse flumeResource;
    public static String NAME = "Flume";
    public static String IMG = "plugins/flume/flume.png";
    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put("hadoop", 1);
    }


    public void init()
    {
        this.flumeResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency = new AngularjsDependency(
                "subutai.plugins.flume",
                "'plugins/flume/flume.js'",
                "'plugins/flume/controller.js'",
                "'plugins/flume/service.js'",
                "'plugins/hadoop/service.js'",
                "'subutai-app/environment/service.js'"
        );

        this.flumeResource.addDependency(angularjsDependency);
    }


    @Override
    public String getModuleInfo()
    {
        return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString() );
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public String getAngularState()
    {
        return this.flumeResource.getAngularjsList();
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.flumeResource.getAngularjsList() );
    }
}
