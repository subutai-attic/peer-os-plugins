package io.subutai.plugin.cassandra.api;


import java.util.Set;

import com.google.common.collect.Sets;

import io.subutai.common.peer.ContainerSize;
import io.subutai.common.settings.Common;
import io.subutai.common.util.CollectionUtil;
import io.subutai.plugin.common.api.ConfigBase;


public class CassandraClusterConfig implements ConfigBase
{

    public static final String PRODUCT_KEY = "Cassandra";
    public static final String PRODUCT_NAME = "Cassandra";
    public static final String PACKAGE_NAME = ( Common.PACKAGE_PREFIX + PRODUCT_NAME ).toLowerCase();

    public static final String TEMPLATE_NAME = "cassandra";
    private String clusterName = "";
    private String domainName = Common.DEFAULT_DOMAIN_NAME;
    private int numberOfSeeds;
    private int numberOfNodes;
    private Set<String> seedNodes;
    private Set<String> nodes;
    private String dataDirectory = "/var/lib/cassandra/data";
    private String commitLogDirectory = "/var/lib/cassandra/commitlog";
    private String savedCachesDirectory = "/var/lib/cassandra/saved_caches";
    private String environmentId;
    private ContainerSize containerType;
    private boolean autoScaling;


    public ContainerSize getContainerType()
    {
        return containerType;
    }


    public void setContainerType( final ContainerSize containerType )
    {
        this.containerType = containerType;
    }


    public boolean isAutoScaling()
    {
        return autoScaling;
    }


    public void setAutoScaling( final boolean autoScaling )
    {
        this.autoScaling = autoScaling;
    }


    public String getEnvironmentId()
    {
        return environmentId;
    }


    public void setEnvironmentId( final String environmentId )
    {
        this.environmentId = environmentId;
    }


    public String getTEMPLATE_NAME()
    {
        return TEMPLATE_NAME;
    }


    public String getClusterName()
    {
        return clusterName;
    }


    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }


    @Override
    public String getProductName()
    {
        return PRODUCT_KEY;
    }


    @Override
    public String getProductKey()
    {
        return PRODUCT_KEY;
    }


    public String getDataDirectory()
    {
        return dataDirectory;
    }


    public void setDataDirectory( String dataDirectory )
    {
        this.dataDirectory = dataDirectory;
    }


    public String getCommitLogDirectory()
    {
        return commitLogDirectory;
    }


    public void setCommitLogDirectory( String commitLogDirectory )
    {
        this.commitLogDirectory = commitLogDirectory;
    }


    public String getSavedCachesDirectory()
    {
        return savedCachesDirectory;
    }


    public void setSavedCachesDirectory( String savedCachesDirectory )
    {
        this.savedCachesDirectory = savedCachesDirectory;
    }


    public int getNumberOfSeeds()
    {
        return numberOfSeeds;
    }


    public void setNumberOfSeeds( int numberOfSeeds )
    {
        this.numberOfSeeds = numberOfSeeds;
    }


    public Set<String> getSeedNodes()
    {
        return seedNodes;
    }


    public void setSeedNodes( Set<String> seedNodes )
    {
        this.seedNodes = seedNodes;
    }


    public Set<String> getNodes()
    {
        return nodes;
    }


    public Set<String> getAllNodes()
    {
        Set<String> allNodes = Sets.newHashSet();
        if ( !CollectionUtil.isCollectionEmpty( nodes ) )
        {
            allNodes.addAll( nodes );
        }
        if ( !CollectionUtil.isCollectionEmpty( seedNodes ) )
        {
            allNodes.addAll( seedNodes );
        }

        return allNodes;
    }


    public void setNodes( Set<String> nodes )
    {
        this.nodes = nodes;
    }


    public String getDomainName()
    {
        return domainName;
    }


    public void setDomainName( String domainName )
    {
        this.domainName = domainName;
    }


    public int getNumberOfNodes()
    {
        return numberOfNodes;
    }


    public void setNumberOfNodes( int numberOfNodes )
    {
        this.numberOfNodes = numberOfNodes;
    }


    @Override
    public String toString()
    {
        return "Config{" +
                "clusterName='" + clusterName + '\'' +
                ", domainName='" + domainName + '\'' +
                ", numberOfSeeds=" + numberOfSeeds +
                ", numberOfNodes=" + numberOfNodes +
                ", dataDirectory='" + dataDirectory + '\'' +
                ", commitLogDirectory='" + commitLogDirectory + '\'' +
                ", savedCachesDirectory='" + savedCachesDirectory + '\'' +
                '}';
    }
}
