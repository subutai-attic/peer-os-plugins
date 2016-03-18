package io.subutai.plugin.flume.impl;

import io.subutai.webui.api.WebuiModule;

public class FlumeWebModule implements WebuiModule
{
    public static String NAME = "Flume";
    public static String IMG = "plugins/flume/flume.png";

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
        return ".state('flume', {\n" + "url: '/plugins/flume',\n"
                + "templateUrl: 'plugins/flume/partials/view.html',\n" +
				"data: {\n" +
				"bodyClass: '',\n" +
				"layout: 'default'\n" +
				"},\n" +
				"resolve: {\n"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "return $ocLazyLoad.load([\n" + "{\n"
                + "name: 'subutai.plugins.flume',\n" + "files: [\n"
                + "'plugins/flume/flume.js',\n" + "'plugins/flume/controller.js',\n"
                + "'plugins/flume/service.js',\n" + "'plugins/hadoop/service.js',\n"
                + "'subutai-app/environment/service.js'\n" + "]\n" + "}\n"
                + "]);\n" + "}]\n" + "}\n" + "})";
    }
}
