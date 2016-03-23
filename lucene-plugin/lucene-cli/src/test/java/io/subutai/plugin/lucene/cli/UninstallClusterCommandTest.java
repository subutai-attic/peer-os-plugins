package io.subutai.plugin.lucene.cli;


import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.lucene.api.Lucene;
import io.subutai.plugin.lucene.api.LuceneConfig;
import io.subutai.plugin.lucene.cli.UninstallClusterCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
    Lucene lucene;
    @Mock
    LuceneConfig luceneConfig;

    @Before
    public void setUp() throws Exception
    {
        uninstallClusterCommand = new UninstallClusterCommand();
        uninstallClusterCommand.setLuceneManager( lucene );
        uninstallClusterCommand.setTracker(tracker);
        when( tracker.getTrackerOperation( anyString(), any(UUID.class) )).thenReturn( trackerOperationView );
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
        uninstallClusterCommand.getLuceneManager();

        // assertions
        assertNotNull(uninstallClusterCommand.getLuceneManager());
        assertEquals( lucene,uninstallClusterCommand.getLuceneManager() );
    }

    @Test
    public void testDoExecute() throws Exception
    {
        when(lucene.uninstallCluster(anyString())).thenReturn( UUID.randomUUID());

        uninstallClusterCommand.doExecute();
    }
}