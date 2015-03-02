package org.safehaus.subutai.plugin.hbase.rest;


import java.util.Set;

import org.safehaus.subutai.common.settings.Common;

import com.google.common.collect.Sets;


public class TrimmedHBaseConfig
{
    private String clusterName;
    private String hadoopClusterName;
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private String environmentId;
    private String hmaster;
    private Set<String> regionServers = Sets.newHashSet();
    private Set<String> quorumPeers = Sets.newHashSet();
    private Set<String> backupMasters = Sets.newHashSet();


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( final String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getHadoopClusterName()
    {
        return hadoopClusterName;
    }


    public void setHadoopClusterName( final String hadoopClusterName )
    {
        this.hadoopClusterName = hadoopClusterName;
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


    public String getHmaster()
    {
        return hmaster;
    }


    public void setHmaster( final String hmaster )
    {
        this.hmaster = hmaster;
    }


    public Set<String> getRegionServers()
    {
        return regionServers;
    }


    public void setRegionServers( final Set<String> regionServers )
    {
        this.regionServers = regionServers;
    }


    public Set<String> getQuorumPeers()
    {
        return quorumPeers;
    }


    public void setQuorumPeers( final Set<String> quorumPeers )
    {
        this.quorumPeers = quorumPeers;
    }


    public Set<String> getBackupMasters()
    {
        return backupMasters;
    }


    public void setBackupMasters( final Set<String> backupMasters )
    {
        this.backupMasters = backupMasters;
    }
}
