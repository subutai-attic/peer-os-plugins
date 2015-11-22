package io.subutai.plugin.cassandra.impl;


import io.subutai.webui.api.WebuiModule;


public class CassandraWebModule implements WebuiModule
{
    public static String NAME = "Cassandra";
    public static String IMG = "plugins/cassandra/cassandra.png";

    public CassandraWebModule()
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
                + "url: '/plugins/cassandra',"
                + "templateUrl: 'subutai-app/plugins/cassandra/partials/view.html',"
                + "resolve: {"
                    + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {" + "return $ocLazyLoad.load(["
                    + "{"
                        + "name: 'subutai.plugins.cassandra',"
                        + "files: ["
                            + "'subutai-app/plugins/cassandra/cassandra.js',"
                            + "'subutai-app/plugins/cassandra/controller.js',"
                            + "'subutai-app/plugins/cassandra/service.js'"
                        + "]"
                    + "}"
                    + "]);"
                + "}]"
                + "}";
    }
}
