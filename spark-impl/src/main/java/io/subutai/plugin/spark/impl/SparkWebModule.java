package io.subutai.plugin.spark.impl;

import io.subutai.webui.api.WebuiModule;

public class SparkWebModule implements WebuiModule
{
    public static String NAME = "Spark";
    public static String IMG = "plugins/spark/spark.png";

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
                + "url: '/plugins/oozie',"
                + "templateUrl: 'plugins/oozie/partials/view.html',"
                + "resolve: {"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
                + "{"
                + "name: 'subutai.plugins.oozie',"
                + "files: ["
                + "'plugins/oozie/oozie.js',"
                + "'plugins/oozie/controller.js',"
                + "'plugins/oozie/service.js'"
                + "]"
                + "}"
                + "]);"
                + "}]"
                + "}";
    }
}
