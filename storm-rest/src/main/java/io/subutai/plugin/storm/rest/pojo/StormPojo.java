package io.subutai.plugin.storm.rest.pojo;


import java.util.Set;


/**
 * Created by ermek on 11/29/15.
 */
public class StormPojo
{
    private String clusterName;
    private String hadoopClusterName;
    private String environmentId;
    private boolean autoScaling;
    private ContainerPojo nimbus;
    private Set<ContainerPojo> supervisors;


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


    public ContainerPojo getNimbus()
    {
        return nimbus;
    }


    public void setNimbus( final ContainerPojo nimbus )
    {
        this.nimbus = nimbus;
    }


    public Set<ContainerPojo> getSupervisors()
    {
        return supervisors;
    }


    public void setSupervisors( final Set<ContainerPojo> supervisors )
    {
        this.supervisors = supervisors;
    }
}
