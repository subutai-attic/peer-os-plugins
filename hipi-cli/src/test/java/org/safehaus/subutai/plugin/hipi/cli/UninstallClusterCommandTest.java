package org.safehaus.subutai.plugin.hipi.cli;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.hipi.api.Hipi;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith( MockitoJUnitRunner.class )
public class UninstallClusterCommandTest
{
    private UninstallClusterCommand uninstallClusterCommand;
    @Mock
    TrackerOperationView trackerOperationView;
    @Mock
    Tracker tracker;
    @Mock
    Hipi hipi;
    @Mock
    HipiConfig hipiClusterConfig;

    @Before
    public void setUp() throws Exception
    {
        uninstallClusterCommand = new UninstallClusterCommand();
        uninstallClusterCommand.setHipiManager( hipi );
        uninstallClusterCommand.setTracker(tracker);
    }

    @Test
    public void testGetTracker() throws Exception
    {
        uninstallClusterCommand.getTracker();

        // assertions
        assertNotNull(uninstallClusterCommand.getTracker());
        assertEquals( tracker,uninstallClusterCommand.getTracker() );
    }

    @Test
    public void testGetPrestoManager() throws Exception
    {
        uninstallClusterCommand.getHipiManager();

        // assertions
        assertNotNull(uninstallClusterCommand.getHipiManager());
        assertEquals( hipi,uninstallClusterCommand.getHipiManager() );
    }

    @Test
    public void testDoExecute() throws Exception
    {
        when(hipi.uninstallCluster(anyString())).thenReturn( UUID.randomUUID());
        when(tracker.getTrackerOperation(anyString(),any(UUID.class))).thenReturn(trackerOperationView);
        when(trackerOperationView.getLog()).thenReturn("test");

        uninstallClusterCommand.doExecute();

        // assertions
        assertNotNull( tracker.getTrackerOperation( anyString(),any(UUID.class) ) );
    }

    @Test
    public void testDoExecuteRunning() throws Exception
    {
        when(hipi.uninstallCluster(anyString())).thenReturn(UUID.randomUUID());
        when(tracker.getTrackerOperation(anyString(),any(UUID.class))).thenReturn(null);
        when(trackerOperationView.getLog()).thenReturn("test");

        uninstallClusterCommand.doExecute();

        //assertions
        assertNull( tracker.getTrackerOperation( anyString(),any(UUID.class) ) );
    }
}