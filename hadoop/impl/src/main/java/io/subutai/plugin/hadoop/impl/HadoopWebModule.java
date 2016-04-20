package io.subutai.plugin.hadoop.impl;


import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


public class HadoopWebModule implements WebuiModule
{
    private WebuiModuleResourse hadoopResource;
    public static String NAME = "Hadoop";
    public static String IMG = "plugins/hadoop/hadoop.png";

    public void init()
    {
        this.hadoopResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency = new AngularjsDependency(
                "subutai.plugins.hadoop",
                "'plugins/hadoop/hadoop.js'",
                "'plugins/hadoop/controller.js'",
                "'plugins/hadoop/service.js'",
                "'subutai-app/environment/service.js'"
        );

        this.hadoopResource.addDependency(angularjsDependency);
    }


    @Override
    public String getModuleInfo()
    {
        return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME );
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public String getAngularState()
    {
        return this.hadoopResource.getAngularjsList();
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), this.hadoopResource.getAngularjsList() );
    }
}
