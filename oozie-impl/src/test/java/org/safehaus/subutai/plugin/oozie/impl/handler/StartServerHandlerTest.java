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
public class StartServerHandlerTest
{
    private StartServerHandler startServerHandler;
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

        startServerHandler = new StartServerHandler(oozieImpl,"testClusterName");

        // assertions
        assertEquals(tracker,oozieImpl.getTracker());
    }

    @Test
    public void testGetTrackerId() throws Exception
    {
        startServerHandler.getTrackerId();

        // assertions
        assertNotNull(startServerHandler.getTrackerId());
        assertEquals(uuid,startServerHandler.getTrackerId());
    }

    @Test
    public void testRun() throws Exception
    {
        startServerHandler.run();
    }
}