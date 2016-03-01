package io.subutai.plugin.cassandra.impl;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.PluginDAO;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class CassandraSetupStrategyTest
{
    private CassandraSetupStrategy cassandraSetupStrategy;
    private String id;
    @Mock
    CassandraImpl cassandraImpl;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    CassandraClusterConfig cassandraClusterConfig;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Environment environment;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    CommandResult commandResult;
    @Mock
    PluginDAO pluginDAO;


    @Before
    public void setUp()
    {
        id = UUID.randomUUID().toString();

        cassandraSetupStrategy =
                new CassandraSetupStrategy( environment, cassandraClusterConfig, trackerOperation, cassandraImpl );
    }


    @Test
    public void testSetup() throws CommandException, ClusterSetupException
    {
        Set<EnvironmentContainerHost> mySet = new HashSet<>();
        mySet.add( containerHost );

        // mock setup method
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );
        when( cassandraClusterConfig.getCommitLogDirectory() ).thenReturn( "test" );
        when( cassandraClusterConfig.getDataDirectory() ).thenReturn( "test" );
        when( cassandraClusterConfig.getSavedCachesDirectory() ).thenReturn( "test" );
        when( cassandraClusterConfig.getDomainName() ).thenReturn( "test" );
        when( cassandraClusterConfig.getProductName() ).thenReturn( "test" );
        when( cassandraClusterConfig.getTEMPLATE_NAME() ).thenReturn( "test" );

        when( cassandraImpl.getCluster( anyString() ) ).thenReturn( null );
        when( environment.getContainerHosts() ).thenReturn( mySet );
        when( containerHost.getId() ).thenReturn( id );

        when( cassandraClusterConfig.getNumberOfSeeds() ).thenReturn( 1 );

        // mock ClusterConfiguration (cofigureCluster method)
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( cassandraImpl.getPluginDAO() ).thenReturn( pluginDAO );
        when( pluginDAO.saveInfo( anyString(), anyString(), any() ) ).thenReturn( true );
        when( environment.getId() ).thenReturn( id );

        cassandraSetupStrategy.setup();

        // asserts
        assertNull( cassandraImpl.getCluster( anyString() ) );
        assertNotNull( environment.getContainerHosts() );
        verify( cassandraClusterConfig ).setEnvironmentId( id );
        assertTrue( pluginDAO.saveInfo( anyString(), anyString(), any() ) );
    }


    @Test( expected = ClusterSetupException.class )
    public void testRunWhenClusterDoesNotExist() throws ClusterSetupException
    {
        when( cassandraClusterConfig.getClusterName() ).thenReturn( "test" );
        when( cassandraClusterConfig.getCommitLogDirectory() ).thenReturn( "test" );
        when( cassandraClusterConfig.getDataDirectory() ).thenReturn( "test" );
        when( cassandraClusterConfig.getSavedCachesDirectory() ).thenReturn( "test" );
        when( cassandraClusterConfig.getDomainName() ).thenReturn( "test" );
        when( cassandraClusterConfig.getProductName() ).thenReturn( "test" );
        when( cassandraClusterConfig.getTEMPLATE_NAME() ).thenReturn( "test" );

        when( cassandraImpl.getCluster( "test" ) ).thenReturn( cassandraClusterConfig );

        cassandraSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testRunWhenMalformedClusterConfiguration() throws ClusterSetupException
    {
        cassandraSetupStrategy.setup();
    }
}