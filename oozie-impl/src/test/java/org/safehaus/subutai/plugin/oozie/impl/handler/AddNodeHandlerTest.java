package org.safehaus.subutai.plugin.oozie.impl.handler;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.oozie.impl.OozieImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AddNodeHandlerTest
{
    private AddNodeHandler addNodeHandler;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;

    @Before
    public void setUp() throws Exception
    {
        when(oozieImpl.getTracker()).thenReturn(tracker);
        when(tracker.createTrackerOperation(anyString(),anyString())).thenReturn(trackerOperation);

        addNodeHandler = new AddNodeHandler(oozieImpl,"testClusterName","testHostName");

        // assertions
        assertEquals(tracker,oozieImpl.getTracker());
    }

    @Test
    public void testRun() throws Exception
    {
        addNodeHandler.run();
    }
}