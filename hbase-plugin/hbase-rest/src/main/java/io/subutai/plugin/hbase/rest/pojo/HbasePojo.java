package io.subutai.plugin.hbase.rest.pojo;


import java.util.Set;


public class HbasePojo
{
    private String clusterName;
    private String hadoopClusterName;
    private String environmentId;
    private boolean autoScaling;
    private Set<ContainerPojo> regionServers;
    private Set<ContainerPojo> quorumPeers;
    private Set<ContainerPojo> backupMasters;
    private ContainerPojo hbaseMaster;


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


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public Set<ContainerPojo> getRegionServers()
    {
        return regionServers;
    }


    public void setRegionServers( final Set<ContainerPojo> regionServers )
    {
        this.regionServers = regionServers;
    }


    public Set<ContainerPojo> getQuorumPeers()
    {
        return quorumPeers;
    }


    public void setQuorumPeers( final Set<ContainerPojo> quorumPeers )
    {
        this.quorumPeers = quorumPeers;
    }


    public Set<ContainerPojo> getBackupMasters()
    {
        return backupMasters;
    }


    public void setBackupMasters( final Set<ContainerPojo> backupMasters )
    {
        this.backupMasters = backupMasters;
    }


    public ContainerPojo getHbaseMaster()
    {
        return hbaseMaster;
    }


    public void setHbaseMaster( final ContainerPojo hbaseMaster )
    {
        this.hbaseMaster = hbaseMaster;
    }
}
