package io.subutai.plugin.lucene.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.lucene.api.LuceneConfig;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class OverHadoopSetupStrategyTest
{
    private OverHadoopSetupStrategy overHadoopSetupStrategy;
    private String id;
    private Set<EnvironmentContainerHost> mySet;
    private Set<String> myUUID;
    private List<String> myList;
    @Mock
    EnvironmentContainerHost containerHost;
    @Mock
    LuceneImpl luceneImpl;
    @Mock
    LuceneConfig luceneConfig;
    @Mock
    Tracker tracker;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Environment environment;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;
    @Mock
    PluginDAO pluginDAO;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    CommandResult commandResult;


    @Before
    public void setUp() throws Exception
    {
        id = UUID.randomUUID().toString();
        myUUID = new HashSet<>();
        myUUID.add( id );

        mySet = new HashSet<>();
        mySet.add( containerHost );

        myList = new ArrayList<>();
        myList.add( id );

        overHadoopSetupStrategy =
                new OverHadoopSetupStrategy( luceneImpl, luceneConfig, trackerOperation, environment );

        when( luceneImpl.getHadoopManager() ).thenReturn( hadoop );
        when( containerHost.execute( any( RequestBuilder.class ) ) ).thenReturn( commandResult );
        when( environment.getContainerHostById( any( String.class ) ) ).thenReturn( containerHost );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupMalformedConfiguration() throws Exception
    {
        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when( luceneConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        when( luceneImpl.getCluster( anyString() ) ).thenReturn( luceneConfig );

        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupContainerIsNotConnected() throws Exception
    {
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        when( environment.getContainerHostsByIds( anySetOf( String.class ) ) ).thenReturn( mySet );

        overHadoopSetupStrategy.setup();

        // assertions
        assertFalse( containerHost.isConnected() );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupFailedToObtainContainers() throws Exception
    {
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        /*when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) )
                .thenThrow( ContainerHostNotFoundException.class );*/


        overHadoopSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNoHadoop() throws Exception
    {
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        //when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.isConnected() ).thenReturn( true );
        when( hadoop.getCluster( anyString() ) ).thenReturn( null );

        overHadoopSetupStrategy.setup();

        // assertions
        assertTrue( containerHost.isConnected() );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNotAllNodesBelongToHadoop() throws Exception
    {
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        //when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.isConnected() ).thenReturn( true );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );

        overHadoopSetupStrategy.setup();

        // assertions
        assertTrue( containerHost.isConnected() );
    }


    @Test
    public void testSetupAlreadyInstalledLucene() throws Exception
    {
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        //when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.isConnected() ).thenReturn( true );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( commandResult.getStdOut() ).thenReturn( Commands.PACKAGE_NAME );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        overHadoopSetupStrategy.setup();

        // assertions
        assertTrue( containerHost.isConnected() );
        verify( trackerOperation ).addLog(
                String.format( "Node %s already has Lucene installed. Omitting this node from installation",
                        containerHost.getHostname() ) );
    }


    @Test
    public void testSetup() throws Exception
    {
        when( luceneConfig.getHadoopClusterName() ).thenReturn( "testHadoopCluster" );
        when( luceneConfig.getNodes() ).thenReturn( myUUID );
        //when( environment.getContainerHostById( UUID.randomUUID() ));
        //when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( mySet );
        when( containerHost.isConnected() ).thenReturn( true );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( commandResult.getStdOut() ).thenReturn( LuceneConfig.PRODUCT_PACKAGE );
        when( commandResult.hasSucceeded() ).thenReturn( true );

        overHadoopSetupStrategy.setup();

        // assertions
        assertTrue( containerHost.isConnected() );
        verify( luceneImpl ).saveConfig( luceneConfig );
    }


    @Test( expected = ClusterSetupException.class )
    public void testProcessResultHasNotSucceeded() throws Exception
    {
        when( commandResult.hasSucceeded() ).thenReturn( false );

        overHadoopSetupStrategy.processResult( containerHost, commandResult );
    }
}