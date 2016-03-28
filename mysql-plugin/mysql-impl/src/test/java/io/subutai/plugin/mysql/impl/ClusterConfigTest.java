package io.subutai.plugin.mysql.impl;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.PluginDAO;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class ClusterConfigTest
{

    TrackerOperation po;
    MySQLCImpl manager;
    ClusterConfig clusterConfig;
    Environment environment;


    @Before
    public void setUp()
    {
        po = mock( TrackerOperation.class );
        manager = mock( MySQLCImpl.class );
        environment = mock( Environment.class );
        clusterConfig = new ClusterConfig( po, manager );
    }


    @Test
    public void testConfigureCluster() throws ContainerHostNotFoundException, ClusterConfigurationException
    {
        EnvironmentContainerHost host = mock( EnvironmentContainerHost.class );
        EnvironmentContainerHost host2 = mock( EnvironmentContainerHost.class );
        Set<EnvironmentContainerHost> mySet = mock( Set.class );
        mySet.add( host );
        mySet.add( host2 );
        String uuid = UUID.randomUUID().toString();
        String uuid2 = UUID.randomUUID().toString();
        List<String> myList = mock( ArrayList.class );
        myList.add( uuid );
        myList.add( uuid );
        Set<String> uuidSet = mock( Set.class );
        uuidSet.add( uuid );
        uuidSet.add( uuid2 );

        PluginDAO pluginDAO = mock( PluginDAO.class );

        MySQLClusterConfig config = mock( MySQLClusterConfig.class );

        Iterator<String> iterator = mock( Iterator.class );
        when( iterator.next() ).thenReturn( uuid ).thenReturn( uuid2 );
        when( config.getDataNodes() ).thenReturn( uuidSet );
        when( config.getManagerNodes() ).thenReturn( uuidSet );
        when( uuidSet.iterator() ).thenReturn( iterator );

        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( environment.getContainerHostById( config.getDataNodes().iterator().next() ) ).thenReturn( host );
        when( environment.getContainerHostById( config.getManagerNodes().iterator().next() ) ).thenReturn( host );

        Iterator<EnvironmentContainerHost> iterator2 = mock( Iterator.class );

        when( mySet.iterator() ).thenReturn( iterator2 );
        when( iterator2.hasNext() ).thenReturn( true ).thenReturn( true ).thenReturn( false ).thenReturn( true )
                                   .thenReturn( false );
        when( iterator2.next() ).thenReturn( host ).thenReturn( host2 ).thenReturn( host );

        Iterator<String> iterator1 = mock( Iterator.class );
        when( uuidSet.iterator() ).thenReturn( iterator1 );
        when( iterator1.hasNext() ).thenReturn( true ).thenReturn( true ).thenReturn( false );
        when( iterator1.next() ).thenReturn( uuid ).thenReturn( uuid2 );
        when( manager.getPluginDAO() ).thenReturn( pluginDAO );
        when( pluginDAO.saveInfo( MySQLClusterConfig.PRODUCT_KEY, config.getClusterName(), config ) )
                .thenReturn( true );

        String uuid1 = UUID.randomUUID().toString();
        when( environment.getId() ).thenReturn( uuid1 );

        clusterConfig.configureCluster( config, environment );

        assertEquals( host, environment.getContainerHostById( config.getDataNodes().iterator().next() ) );
        assertEquals( host, environment.getContainerHostById( config.getManagerNodes().iterator().next() ) );
    }


    @Test
    public void testConstructorConfigureCluster()
    {
        clusterConfig = new ClusterConfig( po, manager );
    }
}
