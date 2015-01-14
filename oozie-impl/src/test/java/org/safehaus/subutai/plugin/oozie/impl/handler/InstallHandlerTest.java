package org.safehaus.subutai.plugin.oozie.impl.handler;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.SetupType;
import org.safehaus.subutai.plugin.oozie.impl.OozieImpl;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class InstallHandlerTest
{
    private InstallHandler installHandler;
    private UUID uuid;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    OozieClusterConfig oozieClusterConfig;
    @Mock
    Tracker tracker;
    @Mock
    TrackerOperation trackerOperation;
    @Mock
    Hadoop hadoop;
    @Mock
    EnvironmentBlueprint environmentBlueprint;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Environment environment;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;
    @Mock
    ClusterSetupStrategy clusterSetupStrategy;

    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID(50, 50);
        when(oozieImpl.getTracker()).thenReturn(tracker);
        when(tracker.createTrackerOperation(anyString(), anyString())).thenReturn(trackerOperation);
        when(oozieClusterConfig.getClusterName()).thenReturn("testClusterName");
        when(trackerOperation.getId()).thenReturn(uuid);
        installHandler = new InstallHandler(oozieImpl, oozieClusterConfig);
        installHandler.setHadoopConfig(hadoopClusterConfig);
    }

    @Test
    public void testGetTrackerId() throws Exception
    {
        installHandler.getTrackerId();

        // assertions
        assertNotNull(installHandler.getTrackerId());
        assertEquals(uuid, installHandler.getTrackerId());
    }

    @Test
    public void testRunWithHadoop() throws ClusterSetupException, EnvironmentBuildException
    {
        when(oozieClusterConfig.getSetupType()).thenReturn(SetupType.WITH_HADOOP);
        when(oozieImpl.getHadoopManager()).thenReturn(hadoop);
        when(hadoop.getDefaultEnvironmentBlueprint(hadoopClusterConfig)).thenReturn(environmentBlueprint);
        when(oozieImpl.getEnvironmentManager()).thenReturn(environmentManager);
        when(environmentManager.getEnvironment(anyString())).thenReturn(environment);

        installHandler.run();

        // assertions
        assertEquals(SetupType.WITH_HADOOP, oozieClusterConfig.getSetupType());
    }

    @Test
    public void testRun()
    {
        when(oozieImpl.getEnvironmentManager()).thenReturn(environmentManager);
        when(environmentManager.getEnvironmentByUUID(any(UUID.class))).thenReturn(environment);
//        when(oozieImpl.getClusterSetupStrategy(environment, oozieClusterConfig, trackerOperation)).thenReturn(clusterSetupStrategy);

        installHandler.run();

        // assertions

    }

    @Test
    public void shouldThrowClusterSetupExceptionInRunWithHadoop()
    {
        when(oozieClusterConfig.getSetupType()).thenReturn(SetupType.WITH_HADOOP);
        when(oozieImpl.getHadoopManager()).thenThrow(ClusterSetupException.class);

        installHandler.run();
    }

    @Test
    public void shouldThrowEnvironmentBuildExceptionInRunWithHadoop()
    {
        when(oozieClusterConfig.getSetupType()).thenReturn(SetupType.WITH_HADOOP);
        when(oozieImpl.getHadoopManager()).thenThrow(EnvironmentBuildException.class);

        installHandler.run();
    }

    @Test
    public void testRunHadoopConfigIsNull()
    {
        installHandler.setHadoopConfig(null);
        when(oozieClusterConfig.getSetupType()).thenReturn(SetupType.WITH_HADOOP);

        installHandler.run();

        // assertions
        verify(trackerOperation).addLogFailed( "No Hadoop configuration specified" );
    }

    @Test
    public void testRunInstallCheckExceptions()
    {
        when(oozieImpl.getEnvironmentManager()).thenReturn(environmentManager);
        when(environmentManager.getEnvironmentByUUID(any(UUID.class))).thenReturn(null);

        installHandler.run();
    }


}