package io.subutai.plugin.spark.rest.pojo;


import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


public class SparkPojo implements Serializable
{
    private String environmentId;
    private String clusterName = "";
    private String hadoopClusterName = "";
    private NodePojo server;
    private Set<NodePojo> clients = new HashSet<>();

    private String environmentDataSource;


    public SparkPojo()
    {
    }


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


    public NodePojo getServer()
    {
        return server;
    }


    public void setServer( final NodePojo nodePojo )
    {
        server = nodePojo;
    }


    public Set<NodePojo> getClients()
    {
        return clients;
    }


    public void setClients( final Set<NodePojo> clients )
    {
        this.clients = clients;
    }


    public String getEnvironmentDataSource()
    {
        return environmentDataSource;
    }


    public void setEnvironmentDataSource( final String environmentDataSource )
    {
        this.environmentDataSource = environmentDataSource;
    }


    @Override
    public boolean equals( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final SparkPojo sparkPojo = ( SparkPojo ) o;

        return getEnvironmentId().equals( sparkPojo.getEnvironmentId() ) && getClusterName()
                .equals( sparkPojo.getClusterName() ) && getHadoopClusterName()
                .equals( sparkPojo.getHadoopClusterName() ) && getServer().equals( sparkPojo.getServer() )
                && getClients().equals( sparkPojo.getClients() );
    }


    @Override
    public int hashCode()
    {
        int result = getEnvironmentId().hashCode();
        result = 31 * result + getClusterName().hashCode();
        result = 31 * result + getHadoopClusterName().hashCode();
        result = 31 * result + getServer().hashCode();
        result = 31 * result + getClients().hashCode();
        return result;
    }


    @Override
    public String toString()
    {
        return "HivePojo{" +
                "environmentId='" + environmentId + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", hadoopClusterName='" + hadoopClusterName + '\'' +
                ", NodePojo='" + server + '\'' +
                ", clients=" + clients +
                '}';
    }
}
