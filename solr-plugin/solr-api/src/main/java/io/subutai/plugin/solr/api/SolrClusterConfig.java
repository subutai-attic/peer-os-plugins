/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.solr.api;


import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Objects;

import io.subutai.common.environment.Topology;
import io.subutai.core.plugincommon.api.ConfigBase;


public class SolrClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Solr";
    public static final String PRODUCT_NAME = "solr";
    public static String TEMPLATE_NAME = PRODUCT_NAME;
    private String clusterName = "";
    private int numberOfNodes = 1;
    private Set<String> nodes = new HashSet<>();
    private String environmentId;
    private Topology environmentTopology;


    public SolrClusterConfig setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
        return this;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    @Override
    public String getProductName()
    {
        return PRODUCT_KEY;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public String getTemplateName()
    {
        return TEMPLATE_NAME;
    }


    public int getNumberOfNodes()
    {
        return nodes.size();
    }


    public void setNumberOfNodes( final int numberOfNodes )
    {
        this.numberOfNodes = numberOfNodes;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }


    public void setNodes( final Set<String> nodes )
    {
        this.nodes = nodes;
    }


    public Topology getEnvironmentTopology()
    {
        return environmentTopology;
    }


    public void setEnvironmentTopology( final Topology environmentTopology )
    {
        this.environmentTopology = environmentTopology;
    }


    @Override
    public String toString()
    {
        return Objects.toStringHelper( this ).add( "templateName", TEMPLATE_NAME ).add( "clusterName", clusterName )
                      .add( "numberOfNodes", numberOfNodes ).add( "nodes", nodes ).add( "environmentId", environmentId )
                      .toString();
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }
}
