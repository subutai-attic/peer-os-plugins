package io.subutai.plugin.zookeeper.impl;


import io.subutai.webui.api.WebuiModule;


public class ZookeeperWebModule implements WebuiModule
{
    public static String NAME = "Zookeeper";
    public static String IMG = "plugins/zookeeper/zookeeper.png";

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
        return ".state('zookeeper', {\n" + "url: '/plugins/zookeeper',\n"
                + "templateUrl: 'plugins/zookeeper/partials/view.html',\n" + "resolve: {\n"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "return $ocLazyLoad.load([\n" + "{\n"
                + "name: 'subutai.plugins.zookeeper',\n" + "files: [\n"
                + "'plugins/zookeeper/zookeeper.js',\n"
                + "'plugins/zookeeper/controller.js',\n"
                + "'plugins/zookeeper/service.js',\n" + "'plugins/hadoop/service.js',\n"
                + "'subutai-app/environment/service.js'\n" + "]\n" + "}\n"
                + "]);\n" + "}]\n" + "}\n" + "})";
    }
}
