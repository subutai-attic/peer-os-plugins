package io.subutai.plugin.hbase.impl;

import io.subutai.webui.api.WebuiModule;

public class HBaseWebModule implements WebuiModule
{
    public static String NAME = "Hbase";
    public static String IMG = "plugins/hbase/hbase.png";

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
        return ".state('hbase', {\n"
                +"    url: '/plugins/hbase',\n"
                +"            templateUrl: 'plugins/hbase/partials/view.html',\n"
                +"            resolve: {\n"
                +"        loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                +"            return $ocLazyLoad.load([\n"
                +"                    {\n"
                +"                            name: 'subutai.plugins.hbase',\n"
                +"                    files: [\n"
                +"            'plugins/hbase/hbase.js',\n"
                +"                    'plugins/hbase/controller.js',\n"
                +"                    'plugins/hbase/service.js',\n"
                +"                    'plugins/hadoop/service.js',\n"
                +"                    'subutai-app/environment/service.js'\n"
                +"            ]\n"
                +"            }\n"
                +"            ]);\n"
                +"        }]}\n"
                +"    })";
    }
}
