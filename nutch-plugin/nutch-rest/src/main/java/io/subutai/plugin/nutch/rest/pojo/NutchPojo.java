package io.subutai.plugin.nutch.rest.pojo;


import java.io.Serializable;
import java.util.Set;


/**
 * Created by ermek on 11/26/15.
 */
public class NutchPojo implements Serializable
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
