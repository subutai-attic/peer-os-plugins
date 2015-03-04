package org.safehaus.subutai.plugin.elasticsearch.cli;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.elasticsearch.api.Elasticsearch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(MockitoJUnitRunner.class)
public class CheckAllNodesCommandTest
{
    private CheckAllNodesCommand checkAllNodesCommand;
    @Mock
    Elasticsearch elasticsearch;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;


    @Before
    public void setUp()
    {
        checkAllNodesCommand = new CheckAllNodesCommand();
    }


    @Test
    public void testGetCassandraManager()
    {
        checkAllNodesCommand.setElasticsearchManager( elasticsearch );
        checkAllNodesCommand.getElasticsearchManager();

        // assertions
        assertNotNull(checkAllNodesCommand.getElasticsearchManager());
        assertEquals(elasticsearch,checkAllNodesCommand.getElasticsearchManager());
    }


    @Test
    public void testSetCassandraManager()
    {
        checkAllNodesCommand.setElasticsearchManager( elasticsearch );
        checkAllNodesCommand.getTracker();

        // assertions
        assertNotNull(checkAllNodesCommand.getElasticsearchManager());
        assertEquals(elasticsearch,checkAllNodesCommand.getElasticsearchManager());
    }


    @Test
    public void testGetTracker()
    {
        checkAllNodesCommand.setTracker(tracker);
        checkAllNodesCommand.getTracker();

        // assertions
        assertNotNull(checkAllNodesCommand.getTracker());
        assertEquals(tracker,checkAllNodesCommand.getTracker());
    }

    @Test
    public void testSetTracker()
    {
        checkAllNodesCommand.setTracker(tracker);
        checkAllNodesCommand.getTracker();

        // assertions
        assertNotNull( checkAllNodesCommand.getTracker() );
        assertEquals( tracker, checkAllNodesCommand.getTracker() );

    }
}