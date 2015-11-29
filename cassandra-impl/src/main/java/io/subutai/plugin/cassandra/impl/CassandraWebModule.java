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
        return ".state('cassandra', {"
                + "    url: '/plugins/cassandra',"
                + "    templateUrl: 'plugins/cassandra/partials/view.html',"
                + "    resolve: {"
                + "    loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {"
                + "        return $ocLazyLoad.load(["
                + "                {"
                + "                        name: 'subutai.plugins.cassandra',"
                + "                files: ["
                + "        'plugins/cassandra/cassandra.js',"
                + "                'plugins/cassandra/controller.js',"
                + "                'plugins/cassandra/service.js',"
                + "                'subutai-app/environment/service.js',"
                + "                'subutai-app/peerRegistration/service.js'"
                + "        ]"
                + "        }"
                + "        ]);"
                + "    }]"
                + "    }"
                + "})";
    }
}
