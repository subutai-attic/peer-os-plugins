package io.subutai.plugin.mahout.rest.pojo;


import java.util.Set;


public class MahoutPojo
{
    private String clusterName;
    private String hadoopClusterName;
    private String environmentId;
    private Set<ContainerPojo> nodes;


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


    public Set<ContainerPojo> getNodes()
    {
        return nodes;
    }


    public void setNodes( final Set<ContainerPojo> nodes )
    {
        this.nodes = nodes;
    }
}
