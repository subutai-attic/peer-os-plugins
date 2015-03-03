package org.safehaus.subutai.plugin.hbase.cli;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hbase.api.HBase;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;

import static java.util.UUID.randomUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class InstallHBaseClusterCommandTest
{
    private InstallHBaseClusterCommand installHBaseClusterCommand;
    private Tracker tracker;
    private HBase hBase;
    private Hadoop hadoop;
    private HadoopClusterConfig config;


    @Before
    public void setUp() throws Exception
    {
        tracker = mock( Tracker.class );
        hBase = mock( HBase.class );
        hadoop = mock( Hadoop.class );
        config = mock( HadoopClusterConfig.class );
        installHBaseClusterCommand = new InstallHBaseClusterCommand();
    }


    @Test
    public void testGetTracker() throws Exception
    {
        installHBaseClusterCommand.setTracker( tracker );
        installHBaseClusterCommand.getTracker();

        // assertions
        assertNotNull( installHBaseClusterCommand.getTracker() );
        assertEquals( tracker, installHBaseClusterCommand.getTracker() );
    }


    @Test
    public void testSetTracker() throws Exception
    {
        installHBaseClusterCommand.setTracker( tracker );
        installHBaseClusterCommand.getTracker();

        // assertions
        assertNotNull( installHBaseClusterCommand.getTracker() );
        assertEquals( tracker, installHBaseClusterCommand.getTracker() );
    }


    @Test
    public void testGetHbaseManager() throws Exception
    {
        installHBaseClusterCommand.setHbaseManager( hBase );
        installHBaseClusterCommand.getHbaseManager();

        // assertions
        assertNotNull( installHBaseClusterCommand.getHbaseManager() );
        assertEquals( hBase, installHBaseClusterCommand.getHbaseManager() );
    }


    @Test
    public void testSetHbaseManager() throws Exception
    {
        installHBaseClusterCommand.setHbaseManager( hBase );
        installHBaseClusterCommand.getHbaseManager();

        // assertions
        assertNotNull( installHBaseClusterCommand.getHbaseManager() );
        assertEquals( hBase, installHBaseClusterCommand.getHbaseManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        when( config.getEnvironmentId() ).thenReturn( randomUUID() );
        when( hadoop.getCluster( anyString() ) ).thenReturn( config );
        installHBaseClusterCommand.setHadoopManager( hadoop );
        installHBaseClusterCommand.setHbaseManager( hBase );
        installHBaseClusterCommand.hmaster = randomUUID().toString();
        String sampleUUID[] = { randomUUID().toString() };
        installHBaseClusterCommand.regionServers = sampleUUID;
        installHBaseClusterCommand.quorumPeers = sampleUUID;
        installHBaseClusterCommand.backupMasters = sampleUUID;
        installHBaseClusterCommand.setTracker( tracker );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) )
                .thenReturn( mock( TrackerOperationView.class ) );
        UUID uuid = randomUUID();
        when( hBase.installCluster( any( HBaseConfig.class ) ) ).thenReturn( uuid );
        installHBaseClusterCommand.doExecute();

        // assertions
        verify( hBase ).installCluster( any( HBaseConfig.class ) );
        when( StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) ).thenReturn( OperationState.FAILED );
        assertEquals( OperationState.FAILED, StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
    }
}



