package io.subutai.plugin.elasticsearch.impl;


import io.subutai.webui.api.WebuiModule;


public class ElasticSearchWebModule implements WebuiModule
{
    public static String NAME = "ElasticSearch";
    public static String IMG = "plugins/elasticsearch/elasticsearch.png";

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
        return ".state('elasticsearch', {" + "url: '/plugins/elasticsearch',\n"
                + "templateUrl: 'plugins/elasticsearch/partials/view.html',\n"
                + "data: {\n"
                + "     bodyClass: '',\n"
                + "     layout: 'default'\n"
                + "},\n"
                + "resolve: {\n"
                    + "loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                        + "return $ocLazyLoad.load([{\n"
                            + "name: 'subutai.plugins.elastic-search',\n" + "files: [\n"
                                + "'plugins/elasticsearch/elastic-search.js',\n"
                                + "'plugins/elasticsearch/controller.js',\n"
                                + "'plugins/elasticsearch/service.js',\n"
                                + "'subutai-app/environment/service.js'\n"
                            + "]}\n"
                    + "]);}]}})";
    }
}
