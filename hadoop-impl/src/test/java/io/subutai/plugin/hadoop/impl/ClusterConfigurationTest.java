package io.subutai.plugin.hadoop.impl;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterConfigurationTest
{
    ClusterConfiguration clusterConfiguration;
    TrackerOperation trackerOperation;
    HadoopImpl hadoopImpl;
    HadoopClusterConfig configBase;
    Environment environment;

    @Before
    public void setUp()
    {
        trackerOperation = mock(TrackerOperation.class);
        hadoopImpl = mock(HadoopImpl.class);
        configBase = mock(HadoopClusterConfig.class);
        environment = mock(Environment.class);
        clusterConfiguration = new ClusterConfiguration(trackerOperation, hadoopImpl);
    }


    @Test
    public void testConfigureCluster() throws ClusterConfigurationException, ContainerHostNotFoundException
    {
        ContainerHost containerHost = mock(ContainerHost.class);
        ContainerHost containerHost2 = mock(ContainerHost.class);
        Set<ContainerHost> mySet = mock(Set.class);
        mySet.add(containerHost);
        mySet.add(containerHost2);

        UUID uuid = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        List<UUID> mylist = mock(ArrayList.class);
        mylist.add(uuid);
        mylist.add(uuid2);

        PluginDAO pluginDAO = mock(PluginDAO.class);
        HadoopClusterConfig hadoopClusterConfig = mock(HadoopClusterConfig.class);
        when(environment.getContainerHostById( hadoopClusterConfig.getNameNode() )).thenReturn(containerHost);
        when(environment.getContainerHostById( hadoopClusterConfig.getJobTracker() )).thenReturn(containerHost);
        when(environment.getContainerHostById( hadoopClusterConfig.getSecondaryNameNode() )).thenReturn(containerHost);

        when(environment.getContainerHosts()).thenReturn(mySet);
        Iterator<ContainerHost> iterator = mock(Iterator.class);
        when(mySet.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        when(iterator.next()).thenReturn(containerHost).thenReturn(containerHost2).thenReturn(containerHost);

        when(hadoopClusterConfig.getDataNodes()).thenReturn(mylist);
        Iterator<UUID> iterator1 = mock(Iterator.class);
        when(mylist.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(iterator1.next()).thenReturn(uuid).thenReturn(uuid2);

        when(hadoopImpl.getPluginDAO()).thenReturn(pluginDAO);
        when(pluginDAO.saveInfo(HadoopClusterConfig.PRODUCT_KEY, configBase.getClusterName(), configBase)).thenReturn
                (true);

        UUID uuid1 = new UUID(50, 50);
        when(environment.getId()).thenReturn(uuid1);
        clusterConfiguration.configureCluster(configBase, environment);

        assertEquals(containerHost, environment.getContainerHostById( hadoopClusterConfig.getNameNode() ));
        assertEquals(containerHost, environment.getContainerHostById( hadoopClusterConfig.getJobTracker() ));
        assertEquals(containerHost, environment.getContainerHostById( hadoopClusterConfig.getSecondaryNameNode() ));
    }

    @Test
    public void testConstructorConfigureCluster()
    {
        clusterConfiguration = new ClusterConfiguration(trackerOperation, hadoopImpl);
    }
}