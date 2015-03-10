package org.safehaus.subutai.plugin.hipi.cli;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hipi.api.Hipi;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class UninstallClusterCommandTest
{
    @Mock TrackerOperationView trackerOperationView;
    @Mock Tracker tracker;
    @Mock Hipi hipi;
    @Mock HipiConfig hipiClusterConfig;
    private UninstallClusterCommand uninstallClusterCommand;


    @Before
    public void setUp() throws Exception
    {
        uninstallClusterCommand = new UninstallClusterCommand();
        uninstallClusterCommand.setHipiManager( hipi );
        uninstallClusterCommand.setTracker( tracker );
    }


    @Test
    public void testGetTracker() throws Exception
    {
        uninstallClusterCommand.getTracker();

        // assertions
        assertNotNull( uninstallClusterCommand.getTracker() );
        assertEquals( tracker, uninstallClusterCommand.getTracker() );
    }


    @Test
    public void testGetHipiManager() throws Exception
    {
        uninstallClusterCommand.getHipiManager();

        // assertions
        assertNotNull( uninstallClusterCommand.getHipiManager() );
        assertEquals( hipi, uninstallClusterCommand.getHipiManager() );
    }


    @Test
    public void testDoExecute() throws Exception
    {
        uninstallClusterCommand.setTracker( tracker );
        uninstallClusterCommand.setHipiManager( hipi );
        UUID uuid = new UUID( 50, 50 );
        when( hipi.uninstallCluster( anyString() ) ).thenReturn( uuid );
        when( tracker.getTrackerOperation( anyString(), any( UUID.class ) ) )
                .thenReturn( mock( TrackerOperationView.class ) );
        when( InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) ).thenReturn( OperationState.FAILED );

        uninstallClusterCommand.doExecute();

        // assertions
        verify( hipi ).uninstallCluster( anyString() );
        assertEquals( OperationState.FAILED, InstallClusterCommand.waitUntilOperationFinish( tracker, uuid ) );
    }
}