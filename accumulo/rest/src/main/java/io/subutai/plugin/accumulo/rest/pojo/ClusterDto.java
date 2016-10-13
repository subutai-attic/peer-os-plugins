package io.subutai.plugin.accumulo.rest.pojo;


import java.util.HashSet;
import java.util.Set;


public class ClusterDto
{
    private String environmentId;
    private String clusterName = "";
    private String hadoopClusterName = "";
    private ContainerDto master;
    private Set<ContainerDto> slaves = new HashSet<>();
    private String environmentDataSource;


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


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


    public ContainerDto getMaster()
    {
        return master;
    }


    public void setMaster( final ContainerDto master )
    {
        this.master = master;
    }


    public Set<ContainerDto> getSlaves()
    {
        return slaves;
    }


    public void setSlaves( final Set<ContainerDto> slaves )
    {
        this.slaves = slaves;
    }


    public String getEnvironmentDataSource()
    {
        return environmentDataSource;
    }


    public void setEnvironmentDataSource( final String environmentDataSource )
    {
        this.environmentDataSource = environmentDataSource;
    }
}
