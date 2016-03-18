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
        return "        .state('hadoop', {\n" +
                "           url: '/plugins/hadoop',\n" +
                "           templateUrl: 'plugins/hadoop/partials/view.html',\n" +
                "           data: {\n" +
                "               bodyClass: '',\n" +
                "               layout: 'default'\n" +
                "           },\n" +
                "            resolve: {\n" +
                "                loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
                "                    return $ocLazyLoad.load([\n" +
                "                        {\n" +
                "                            name: 'subutai.plugins.hadoop',\n" +
                "                            files: [\n" +
                "                                'plugins/hadoop/hadoop.js',\n" +
                "                                'plugins/hadoop/controller.js',\n" +
                "                                'plugins/hadoop/service.js',\n" +
                "                                'subutai-app/environment/service.js'\n" +
                "                            ]\n" +
                "                        }\n" +
                "                    ]);\n" +
                "                }]\n" +
                "            }\n" +
                "        })";
    }
}
