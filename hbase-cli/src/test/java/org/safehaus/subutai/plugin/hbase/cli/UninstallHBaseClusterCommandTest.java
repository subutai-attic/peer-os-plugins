package org.safehaus.subutai.plugin.hbase.cli;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hbase.api.HBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class UninstallHBaseClusterCommandTest
{
    private Tracker tracker;
    private HBase hBase;
    private UninstallHBaseClusterCommand uninstallHBaseClusterCommand;


    @Before
    public void setUp() throws Exception
    {
        tracker = mock( Tracker.class );
        hBase = mock( HBase.class );

        uninstallHBaseClusterCommand = new UninstallHBaseClusterCommand();
    }


    @Test
    public void testGetTracker() throws Exception
    {
        uninstallHBaseClusterCommand.setTracker( tracker );
        uninstallHBaseClusterCommand.getTracker();

        // assertions
        assertNotNull( uninstallHBaseClusterCommand.getTracker() );
        assertEquals( tracker, uninstallHBaseClusterCommand.getTracker() );
    }


    @Test
    public void testSetTracker() throws Exception
    {
        uninstallHBaseClusterCommand.setTracker( tracker );
        uninstallHBaseClusterCommand.getTracker();

        // assertions
        assertNotNull( uninstallHBaseClusterCommand.getTracker() );
        assertEquals( tracker, uninstallHBaseClusterCommand.getTracker() );
    }


    @Test
    public void testGetHbaseManager() throws Exception
    {
        uninstallHBaseClusterCommand.setHbaseManager( hBase );
        uninstallHBaseClusterCommand.getHbaseManager();

        // assertions
        assertNotNull( uninstallHBaseClusterCommand.getHbaseManager() );
        assertEquals( hBase, uninstallHBaseClusterCommand.getHbaseManager() );
    }


    @Test
    public void testSetHbaseManager() throws Exception
    {
        uninstallHBaseClusterCommand.setHbaseManager( hBase );
        uninstallHBaseClusterCommand.getHbaseManager();

        // assertions
        assertNotNull( uninstallHBaseClusterCommand.getHbaseManager() );
        assertEquals( hBase, uninstallHBaseClusterCommand.getHbaseManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        uninstallHBaseClusterCommand.setTracker( tracker );
        uninstallHBaseClusterCommand.setHbaseManager( hBase );
        UUID uuid = new UUID( 50, 50 );
        when( hBase.uninstallCluster( anyString() ) ).thenReturn( uuid );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) )
                .thenReturn( mock( TrackerOperationView.class ) );

        uninstallHBaseClusterCommand.doExecute();

        // assertions
        verify( hBase ).uninstallCluster( anyString() );
        when( StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) ).thenReturn( OperationState.FAILED );
        assertEquals( OperationState.FAILED, StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
    }
}