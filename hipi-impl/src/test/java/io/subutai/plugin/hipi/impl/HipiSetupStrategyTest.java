package io.subutai.plugin.hipi.impl;


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
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hipi.api.HipiConfig;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class HipiSetupStrategyTest
{
    @Mock HadoopClusterConfig hadoopClusterConfig;
    @Mock Hadoop hadoop;
    @Mock CommandResult commandResult;
    @Mock ContainerHost containerHost;
    @Mock
    HipiImpl hipiImpl;
    @Mock HipiConfig hipiConfig;
    @Mock Tracker tracker;
    @Mock EnvironmentManager environmentManager;
    @Mock TrackerOperation trackerOperation;
    @Mock Environment environment;
    private HipiSetupStrategy hipiSetupStrategy;
    private UUID uuid;
    private Set<UUID> mySet;
    private List<UUID> myList;
    private Set<ContainerHost> myCont;
    private List<HipiConfig> myHipi;


    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID( 50, 50 );
        mySet = new HashSet<>();
        mySet.add( uuid );
        mySet.add( uuid );

        myList = new ArrayList<>();
        myList.add( uuid );

        myCont = new HashSet<>();
        myCont.add( containerHost );

        myHipi = new ArrayList<>();
        myHipi.add( hipiConfig );

        hipiSetupStrategy = new HipiSetupStrategy( hipiImpl, hipiConfig, trackerOperation );

        when( hipiImpl.getHadoopManager() ).thenReturn( hadoop );
        when( hipiImpl.getEnvironmentManager() ).thenReturn( environmentManager );
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupInvalidClusterName() throws Exception
    {
        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupInvalidHadoopClusterName() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNodesNotSpecified() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupClusterAlreadyExist() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( hipiImpl.getCluster( anyString() ) ).thenReturn( hipiConfig );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNoHadoopCluster() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( hadoop.getCluster( anyString() ) ).thenReturn( null );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNoEnvironment() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenThrow( EnvironmentNotFoundException.class );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupExpectedNodes() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNotAllNodesBelongToHadoop() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );

        hipiSetupStrategy.setup();
    }


    @Test( expected = ClusterSetupException.class )
    public void testSetupNodeIsNotConnected() throws Exception
    {
        when( hipiConfig.getClusterName() ).thenReturn( "testClusterName" );
        when( hipiConfig.getHadoopClusterName() ).thenReturn( "testHadoopClusterName" );
        when( hipiConfig.getNodes() ).thenReturn( mySet );
        when( hadoop.getCluster( anyString() ) ).thenReturn( hadoopClusterConfig );
        when( environmentManager.findEnvironment( any( UUID.class ) ) ).thenReturn( environment );
        when( hadoopClusterConfig.getAllNodes() ).thenReturn( myList );
        when( environment.getContainerHostsByIds( anySetOf( UUID.class ) ) ).thenReturn( myCont );

        hipiSetupStrategy.setup();
    }
}