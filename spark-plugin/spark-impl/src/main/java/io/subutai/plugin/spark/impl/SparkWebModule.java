package io.subutai.plugin.spark.impl;

import io.subutai.webui.api.WebuiModule;

public class SparkWebModule implements WebuiModule {
    public static String NAME = "Spark";
    public static String IMG = "plugins/spark/spark.png";

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
        return ".state('spark', {\n"
                + "url: '/plugins/spark',\n"
                + "templateUrl: 'plugins/spark/partials/view.html',\n"
                + "data: {\n"
                + "     bodyClass: '',\n"
                + "     layout: 'default'\n"
                + "},\n"
                + "resolve: {\n"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "return $ocLazyLoad.load([\n"
                + "{\n"
                + "name: 'subutai.plugins.spark',\n"
                + "files: [\n"
                + "'plugins/spark/spark.js',\n"
                + "'plugins/spark/controller.js',\n"
                + "'plugins/spark/service.js',\n"
                + "'plugins/hadoop/service.js',\n"
                + "'subutai-app/environment/service.js'\n"
                + "]\n"
                + "}\n"
                + "]);\n"
                + "}]\n"
                + "}\n"
                + "})";
    }
}
