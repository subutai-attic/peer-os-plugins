package io.subutai.plugin.hadoop.impl.handler;


import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.metric.api.Monitor;
import io.subutai.core.strategy.api.StrategyManager;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.common.api.PluginDAO;
import io.subutai.plugin.hadoop.impl.HadoopImpl;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Ignore
public class RemoveNodeOperationHandlerTest
{
    RemoveNodeOperationHandler removeNodeOperationHandler;
    TrackerOperation trackerOperation;
    UUID uuid;
    ExecutorService executorService;
    Monitor monitor;

    @Mock
    PluginDAO pluginDAO;

    @Mock
    private StrategyManager strategyManager;


    @Before
    public void setUp()
    {
        executorService = mock( ExecutorService.class );
        trackerOperation = mock( TrackerOperation.class );
        monitor = mock( Monitor.class );
        uuid = new UUID( 50, 50 );
        Tracker tracker = mock( Tracker.class );
        String clusterName = "test";
        String lxcHostName = "test";
        HadoopImpl hadoop = new HadoopImpl( strategyManager, monitor, pluginDAO );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        hadoop.setTracker( tracker );
        hadoop.setExecutor( executorService );

        removeNodeOperationHandler = new RemoveNodeOperationHandler( hadoop, clusterName, lxcHostName );

        assertEquals( uuid, trackerOperation.getId() );
        assertEquals( tracker, hadoop.getTracker() );
        assertEquals( executorService, hadoop.getExecutor() );
    }


    @Test
    public void testRun()
    {
        Tracker tracker = mock( Tracker.class );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        HadoopImpl hadoop = new HadoopImpl( strategyManager, monitor, pluginDAO );
        when( trackerOperation.getId() ).thenReturn( uuid );
        when( tracker.createTrackerOperation( anyString(), anyString() ) ).thenReturn( trackerOperation );
        hadoop.setTracker( tracker );
        hadoop.setExecutor( executorService );
        removeNodeOperationHandler.run();

        assertEquals( uuid, trackerOperation.getId() );
        assertEquals( tracker, hadoop.getTracker() );
        assertEquals( executorService, hadoop.getExecutor() );
    }
}