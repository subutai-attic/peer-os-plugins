package io.subutai.plugin.cassandra.cli;


import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.cassandra.api.Cassandra;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.cli.StopAllNodesCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StopAllNodesCommandTest
{
    private StopAllNodesCommand stopAllNodesCommand;
    @Mock
    Cassandra cassandra;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;

    @Before
    public void setUp() 
    {
        stopAllNodesCommand = new StopAllNodesCommand();
    }

    @Test
    public void testGetCassandraManager() 
    {
        stopAllNodesCommand.setCassandraManager(cassandra);
        stopAllNodesCommand.getCassandraManager();

        // assertions
        assertNotNull(stopAllNodesCommand.getCassandraManager());
        assertEquals(cassandra,stopAllNodesCommand.getCassandraManager());

    }

    @Test
    public void testSetCassandraManager() 
    {
        stopAllNodesCommand.setCassandraManager(cassandra);
        stopAllNodesCommand.getCassandraManager();

        // assertions
        assertNotNull(stopAllNodesCommand.getCassandraManager());
        assertEquals(cassandra,stopAllNodesCommand.getCassandraManager());
        
    }

    @Test
    public void testGetTracker() 
    {
        stopAllNodesCommand.setTracker(tracker);
        stopAllNodesCommand.getTracker();

        // assertions
        assertNotNull(stopAllNodesCommand.getTracker());
        assertEquals(tracker,stopAllNodesCommand.getTracker());

    }

    @Test
    public void testSetTracker() 
    {
        stopAllNodesCommand.setTracker(tracker);
        stopAllNodesCommand.getTracker();

        // assertions
        assertNotNull(stopAllNodesCommand.getTracker());
        assertEquals(tracker,stopAllNodesCommand.getTracker());

    }

    @Test
    public void testDoExecute() throws IOException
    {
        UUID uuid = new UUID(50,50);
        stopAllNodesCommand.setTracker(tracker);
        stopAllNodesCommand.setCassandraManager(cassandra);
        when(cassandra.stopCluster(anyString())).thenReturn(uuid);
        when(tracker.getTrackerOperation(CassandraClusterConfig.PRODUCT_KEY, uuid)).thenReturn(trackerOperationView);
        when(trackerOperationView.getLog()).thenReturn("test");

        stopAllNodesCommand.doExecute();

        // assertions
        assertNotNull(tracker.getTrackerOperation(CassandraClusterConfig.PRODUCT_KEY,uuid));
        verify(cassandra).stopCluster(anyString());
        assertNotEquals(OperationState.RUNNING,trackerOperationView.getState());
    }

    @Test
    public void testDoExecuteWhenTrackerOperationViewIsNull() throws IOException
    {
        UUID uuid = new UUID(50,50);
        stopAllNodesCommand.setTracker(tracker);
        stopAllNodesCommand.setCassandraManager(cassandra);
        when(cassandra.stopCluster(anyString())).thenReturn(uuid);
        when(tracker.getTrackerOperation(CassandraClusterConfig.PRODUCT_KEY,uuid)).thenReturn(null);

        stopAllNodesCommand.doExecute();

        // assertions
        assertNull(tracker.getTrackerOperation(CassandraClusterConfig.PRODUCT_KEY,uuid));
        verify(cassandra).stopCluster(anyString());
    }

}