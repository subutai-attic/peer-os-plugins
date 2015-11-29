package io.subutai.plugin.elasticsearch.cli;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.elasticsearch.api.Elasticsearch;
import io.subutai.plugin.elasticsearch.cli.CheckAllNodesCommand;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

//@RunWith(MockitoJUnitRunner.class)
public class CheckAllNodesCommandTest
{
    private CheckAllNodesCommand checkAllNodesCommand;
    @Mock
    Elasticsearch elasticsearch;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperationView trackerOperationView;


//    @Before
//    public void setUp()
//    {
//        checkAllNodesCommand = new CheckAllNodesCommand();
//    }
}