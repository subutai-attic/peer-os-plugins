package io.subutai.plugin.pig.impl;


import io.subutai.webui.api.WebuiModule;


public class PigWebModule implements WebuiModule
{
    public static String NAME = "Pig";
    public static String IMG = "plugins/pig/pig.png";

    @Override
    public String getName() {
        return NAME;
    }


    @Override
    public String getModuleInfo() {
        return String.format("{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME);
    }


    @Override
    public String getAngularDependecyList() {
        return ".state('pig', {\n" + "url: '/plugins/pig',\n"
                + "templateUrl: 'plugins/pig/partials/view.html',\n" + "resolve: {\n"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "return $ocLazyLoad.load([\n" + "{\n"
                + "name: 'subutai.plugins.pig',\n" + "files: [\n"
                + "'plugins/pig/pig.js',\n" + "'plugins/pig/controller.js',\n"
                + "'plugins/pig/service.js',\n" + "'plugins/hadoop/service.js',\n"
                + "'subutai-app/environment/service.js'\n" + "]\n" + "}\n"
                + "]);\n" + "}]\n" + "}\n" + "})";
    }
}
