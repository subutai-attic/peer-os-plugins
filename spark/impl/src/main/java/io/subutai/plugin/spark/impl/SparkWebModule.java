package io.subutai.plugin.spark.impl;


import com.google.gson.Gson;
import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;

import java.util.HashMap;
import java.util.Map;

public class SparkWebModule implements WebuiModule {
    public static String NAME = "Spark";
    public static String IMG = "plugins/spark/spark.png";

    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;
    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put("hadoop", 1);
    }


    private WebuiModuleResourse sparkResource;


    public void init()
    {
        sparkResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency = new AngularjsDependency(
                "subutai.plugins.spark",
                "plugins/spark/spark.js",
                "plugins/spark/controller.js",
                "plugins/spark/service.js",
                "plugins/hadoop/service.js",
                "subutai-app/environment/service.js"
        );

        sparkResource.addDependency(angularjsDependency);
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
        return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME, new Gson().toJson( TEMPLATES_REQUIREMENT ).toString());
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), sparkResource.getAngularjsList() );
    }
}
