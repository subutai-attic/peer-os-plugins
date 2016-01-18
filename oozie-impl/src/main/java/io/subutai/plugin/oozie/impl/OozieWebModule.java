package io.subutai.plugin.oozie.impl;


import io.subutai.webui.api.WebuiModule;


public class OozieWebModule implements WebuiModule
{
    public static String NAME = "Oozie";
    public static String IMG = "plugins/oozie/oozie.png";


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
        return ".state('oozie', {\n" +
                "url: '/plugins/oozie',\n" +
                "templateUrl: 'plugins/oozie/partials/view.html',\n" +
                "data: {\n" +
                "bodyClass: '',\n" +
                "layout: 'default'\n" +
                "},\n" +
                "resolve: {\n" +
                "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n" +
                "return $ocLazyLoad.load([\n" +
                "{\n" +
                "name: 'subutai.plugins.oozie',\n" +
                "files: [\n" +
                "'plugins/oozie/oozie.js',\n" +
                "'plugins/oozie/controller.js',\n" +
                "'plugins/oozie/service.js',\n" +
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
