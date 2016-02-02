package io.subutai.plugin.solr.impl;


import io.subutai.webui.api.WebuiModule;


public class SolrWebModule implements WebuiModule
{
    public static String NAME = "Solr";
    public static String IMG = "plugins/solr/solr.png";


    public String getName()
    {
        return NAME;
    }


    public String getModuleInfo()
    {
        return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME );
    }


    public String getAngularDependecyList()
    {
        return ".state('solr', {\n" + "url: '/plugins/solr',\n"
                + "templateUrl: 'plugins/solr/partials/view.html',\n"
                + "data: {\n"
                + "     bodyClass: '',\n"
                + "     layout: 'default'\n"
                + "},\n"
                + "resolve: {\n"
                + "        loadPlugin: ['$ocLazyLoad', function ($ocLazyLoad) {\n"
                + "                return $ocLazyLoad.load([\n"
                + "{\n"
                + "        name: 'subutai.plugins.solr',\n"
                + "        files: [\n"
                + "                'plugins/solr/solr.js',\n"
                + "                'plugins/solr/controller.js',\n"
                + "                'plugins/solr/service.js',\n"
                + "                'subutai-app/environment/service"
                + ".js'\n"
                + "        ]\n"
                + "}\n"
                + "                ]);\n" + "        }]\n"
                + "}\n" + "                })";
    }
}
