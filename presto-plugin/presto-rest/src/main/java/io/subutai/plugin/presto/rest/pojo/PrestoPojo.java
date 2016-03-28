package io.subutai.plugin.presto.rest.pojo;


import java.util.Set;


public class PrestoPojo
{
    private String clusterName;
    private String hadoopClusterName;
    private String environmentId;
    private boolean autoScaling;
    private ContainerPojo coordinator;
    private Set<ContainerPojo> workers;


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


    public ContainerPojo getCoordinator()
    {
        return coordinator;
    }


    public void setCoordinator( final ContainerPojo coordinator )
    {
        this.coordinator = coordinator;
    }


    public Set<ContainerPojo> getWorkers()
    {
        return workers;
    }


    public void setWorkers( final Set<ContainerPojo> workers )
    {
        this.workers = workers;
    }
}
