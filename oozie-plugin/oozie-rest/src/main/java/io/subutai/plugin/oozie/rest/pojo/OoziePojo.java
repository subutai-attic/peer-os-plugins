package io.subutai.plugin.oozie.rest.pojo;


import java.util.Set;


/**
 * Created by ermek on 11/27/15.
 */
public class OoziePojo
{
    private String clusterName;
    private String hadoopClusterName;
    private String environmentId;
    private boolean autoScaling;
    private ContainerPojo server;
    private Set<ContainerPojo> clients;


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


    public Set<ContainerPojo> getClients()
    {
        return clients;
    }


    public void setClients( final Set<ContainerPojo> clients )
    {
        this.clients = clients;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public ContainerPojo getServer()
    {
        return server;
    }


    public void setServer( final ContainerPojo server )
    {
        this.server = server;
    }
}
