/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.solr.api;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;


import org.safehaus.subutai.plugin.common.api.ConfigBase;

import com.google.common.base.Objects;


public class SolrClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Solr";
    public static final String PRODUCT_NAME = "solr";
    private String templateName = PRODUCT_NAME;
    private String clusterName = "";
    private int numberOfNodes = 1;
    private Set<UUID> nodes = new HashSet<>();
    private UUID environmentId;


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
        return templateName;
    }


    public void setTemplateName( final String templateName )
    {
        this.templateName = templateName;
    }


    public int getNumberOfNodes()
    {
        return numberOfNodes;
    }


    public void setNumberOfNodes( final int numberOfNodes )
    {
        this.numberOfNodes = numberOfNodes;
    }


    public Set<UUID> getNodes()
    {
        return nodes;
    }


    public void setNodes( final Set<UUID> nodes )
    {
        this.nodes = nodes;
    }


    @Override
    public String toString()
    {
        return Objects.toStringHelper( this ).add( "templateName", templateName ).add( "clusterName", clusterName )
                      .add( "numberOfNodes", numberOfNodes ).add( "nodes", nodes ).add( "environmentId", environmentId )
                      .toString();
    }


    public UUID getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final UUID environmentId )
    {
        this.environmentId = environmentId;
    }
}
