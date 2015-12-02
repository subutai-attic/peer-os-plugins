package io.subutai.plugin.hipi.impl;

import io.subutai.webui.api.WebuiModule;


public class HipiWebModule implements WebuiModule
{
    public static String NAME = "Hipi";
    public static String IMG = "plugins/hipi/hipi.png";

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
        return ".state('hipi', {\n" +
                "url: '/plugins/hipi',\n" +
                "templateUrl: 'plugins/hipi/partials/view.html',\n" +
                "resolve: {\n" +
                "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
                "return $ocLazyLoad.load([\n" +
                "{\n" +
                "name: 'subutai.plugins.hipi',\n" +
                "files: [\n" +
                "'plugins/hipi/hipi.js',\n" +
                "'plugins/hipi/controller.js',\n" +
                "'plugins/hipi/service.js',\n" +
                "'plugins/hadoop/service.js',\n" +
                "'subutai-app/environment/service.js'\n" +
                "]\n" +
                "}\n" +
                "]);\n" +
                "}]\n" +
                "}\n" +
                "})";
    }
}
