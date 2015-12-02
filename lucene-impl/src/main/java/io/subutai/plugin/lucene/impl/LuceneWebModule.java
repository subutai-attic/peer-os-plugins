package io.subutai.plugin.lucene.impl;


import io.subutai.webui.api.WebuiModule;


public class LuceneWebModule implements WebuiModule
{
    public static String NAME = "Lucene";
    public static String IMG = "plugins/lucene/lucene.png";

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
        return ".state('lucene', {\n" + "url: '/plugins/lucene',\n"
                + "templateUrl: 'plugins/lucene/partials/view.html',\n" + "resolve: {\n"
                + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "return $ocLazyLoad.load([\n" + "{\n"
                + "name: 'subutai.plugins.lucene',\n" + "files: [\n"
                + "'plugins/lucene/lucene.js',\n" + "'plugins/lucene/controller.js',\n"
                + "'plugins/lucene/service.js',\n" + "'plugins/hadoop/service.js',\n"
                + "'subutai-app/environment/service.js'\n" + "]\n" + "}\n"
                + "]);\n" + "}]\n" + "}\n" + "})";
    }
}
