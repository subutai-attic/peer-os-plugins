package org.safehaus.subutai.plugin.oozie.impl.handler;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.oozie.impl.OozieImpl;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DestroyNodeOperationHandlerTest
{
    private DestroyNodeOperationHandler destroyNodeOperationHandler;
    private UUID uuid;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;

    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID(50,50);
        when(oozieImpl.getTracker()).thenReturn(tracker);
        when(tracker.createTrackerOperation(anyString(),anyString())).thenReturn(trackerOperation);
        when(trackerOperation.getId()).thenReturn(uuid);

        destroyNodeOperationHandler= new DestroyNodeOperationHandler(oozieImpl,"testClusterName","testHostName");

        // assertions
        assertEquals(tracker,oozieImpl.getTracker());
    }

    @Test
    public void testGetTrackerId() throws Exception
    {
        destroyNodeOperationHandler.getTrackerId();

        // assertions
        assertNotNull(destroyNodeOperationHandler.getTrackerId());
        assertEquals(uuid,destroyNodeOperationHandler.getTrackerId());
    }

    @Test
    public void testRun() throws Exception
    {
        destroyNodeOperationHandler.run();
    }
}