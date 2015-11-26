package io.subutai.plugin.solr.impl;


import io.subutai.webui.api.WebuiModule;


public class SolrWebModule implements WebuiModule
{
    public static String NAME = "Solr";
    public static String IMG = "plugins/solr/solr.png";

    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public String getModuleInfo()
    {
        return String.format("{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME);
    }


    @Override
    public String getAngularDependecyList()
    {
        return null;
    }
}
