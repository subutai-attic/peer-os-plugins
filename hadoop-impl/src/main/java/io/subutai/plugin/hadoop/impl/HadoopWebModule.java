package io.subutai.plugin.hadoop.impl;


import io.subutai.webui.api.WebuiModule;


public class HadoopWebModule implements WebuiModule
{
    public static String NAME = "Hadoop";
    public static String IMG = "plugins/hadoop/hadoop.png";

    public HadoopWebModule()
    {

    }

    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public String getModuleInfo()
    {
        return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME );
    }


    @Override
    public String getAngularDependecyList()
    {
        return "{"
                + "url: '/plugins/hadoop',"
                + "templateUrl: 'plugins/hadoop/partials/view.html',"
                + "resolve: {"
                    + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
                    + "{"
                        + "name: 'subutai.plugins.cassandra',"
                        + "files: ["
                            + "'plugins/hadoop/cassandra.js',"
                            + "'plugins/hadoop/controller.js',"
                            + "'plugins/hadoop/service.js'"
                        + "]"
                    + "}"
                    + "]);"
                + "}]"
                + "}";
    }
}
