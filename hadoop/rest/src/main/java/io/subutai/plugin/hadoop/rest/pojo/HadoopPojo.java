package io.subutai.plugin.hadoop.rest.pojo;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;

import io.subutai.common.settings.Common;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;


public class HadoopPojo implements Serializable
{
    private String clusterName, domainName;
    private ContainerPojo nameNode;
    private String replicationFactor;
    private Set<ContainerPojo> slaves;
    private String environmentId;
    private String environmentDataSource;
    private boolean autoScaling;


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( final String domainName )
    {
        this.domainName = domainName;
    }


    public ContainerPojo getNameNode()
    {
        return nameNode;
    }


    public void setNameNode( final ContainerPojo nameNode )
    {
        this.nameNode = nameNode;
    }


    public String getReplicationFactor()
    {
        return replicationFactor;
    }


    public void setReplicationFactor( final String replicationFactor )
    {
        this.replicationFactor = replicationFactor;
    }


    public Set<ContainerPojo> getSlaves()
    {
        return slaves;
    }


    public void setSlaves( final Set<ContainerPojo> slaves )
    {
        this.slaves = slaves;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getEnvironmentDataSource()
    {
        return environmentDataSource;
    }


    public void setEnvironmentDataSource( final String environmentDataSource )
    {
        this.environmentDataSource = environmentDataSource;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }
}
