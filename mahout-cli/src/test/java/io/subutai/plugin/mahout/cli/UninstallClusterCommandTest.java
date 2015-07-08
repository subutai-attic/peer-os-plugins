package io.subutai.plugin.mahout.cli;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.mahout.api.Mahout;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.plugin.mahout.cli.UninstallClusterCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class UninstallClusterCommandTest
{
    private UninstallClusterCommand uninstallClusterCommand;
    @Mock
    TrackerOperationView trackerOperationView;
    @Mock
    Tracker tracker;
    @Mock
    Mahout mahout;
    @Mock
    MahoutClusterConfig mahoutClusterConfig;

    @Before
    public void setUp() throws Exception
    {
        uninstallClusterCommand = new UninstallClusterCommand();
        uninstallClusterCommand.setMahoutManager( mahout);
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
    public void testGetMahoutManager() throws Exception
    {
        uninstallClusterCommand.getMahoutManager();

        // assertions
        assertNotNull(uninstallClusterCommand.getMahoutManager());
        assertEquals( mahout,uninstallClusterCommand.getMahoutManager() );
    }

    @Test
    public void testDoExecute() throws Exception
    {
        when(mahout.uninstallCluster(anyString())).thenReturn(UUID.randomUUID());
        when(tracker.getTrackerOperation(anyString(),any(UUID.class))).thenReturn(trackerOperationView);
        when(trackerOperationView.getLog()).thenReturn("test");

        uninstallClusterCommand.doExecute();
    }

    @Test
    public void testDoExecuteRunning() throws Exception
    {
        when(mahout.uninstallCluster(anyString())).thenReturn(UUID.randomUUID());
        when(tracker.getTrackerOperation(anyString(),any(UUID.class))).thenReturn(null);
        when(trackerOperationView.getLog()).thenReturn("test");

        uninstallClusterCommand.doExecute();
    }

}