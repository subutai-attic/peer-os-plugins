package io.subutai.plugin.hadoop.rest;


import java.util.Set;

import com.google.common.collect.Sets;

import io.subutai.common.settings.Common;


public class TrimmedHadoopConfig
{
    private String clusterName;
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private String environmentId;
    private String replicationFactor;
    private String nameNode;
    private String jobTracker;
    private String secNameNode;
    private Set<String> slaves = Sets.newHashSet();


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


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public void setReplicationFactor( String replicationFactor )
    {
        this.replicationFactor = replicationFactor;
    }


    public String getReplicationFactor()
    {
        return replicationFactor;
    }


    public String getNameNode()
    {
        return nameNode;
    }


    public void setNameNode( final String nameNode )
    {
        this.nameNode = nameNode;
    }


    public String getJobTracker()
    {
        return jobTracker;
    }


    public void setJobTracker( final String jobTracker )
    {
        this.jobTracker = jobTracker;
    }


    public String getSecNameNode()
    {
        return secNameNode;
    }


    public void setSecNameNode( final String secNameNode )
    {
        this.secNameNode = secNameNode;
    }


    public Set<String> getSlaves()
    {
        return slaves;
    }


    public void setSlaves( final Set<String> slaves )
    {
        this.slaves = slaves;
    }
}
