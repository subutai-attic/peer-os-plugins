package io.subutai.plugin.oozie.impl.alert;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.core.env.api.EnvironmentManager;
import io.subutai.core.metric.api.ContainerHostMetric;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;
import io.subutai.plugin.oozie.impl.alert.AlertException;
import io.subutai.plugin.oozie.impl.alert.OozieAlertListener;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class OozieAlertListenerTest
{
    private OozieAlertListener oozieAlertListener;
    private UUID uuid;
    @Mock
    RequestBuilder requestBuilder;
    @Mock
    Commands commands;
    @Mock
    CommandResult commandResult;
    @Mock
    Host host;
    @Mock
    CommandUtil commandUtil;
    @Mock
    ContainerHost containerHost;
    @Mock
    OozieClusterConfig oozieClusterConfig;
    @Mock
    OozieImpl oozieImpl;
    @Mock
    ContainerHostMetric containerHostMetric;
    @Mock
    EnvironmentManager environmentManager;
    @Mock
    Environment environment;
    @Mock
    MonitoringSettings monitoringSettings;
    @Mock
    ProcessResourceUsage processResourceUsage;
    @Mock
    Hadoop hadoop;
    @Mock
    HadoopClusterConfig hadoopClusterConfig;

    @Before
    public void setUp() throws Exception
    {
        uuid = new UUID(50, 50);
        oozieAlertListener = new OozieAlertListener(oozieImpl);
    }

    @Test(expected = AlertException.class)
    public void testOnAlertClusterNotFound() throws Exception
    {
        oozieAlertListener.onAlert(containerHostMetric);
    }


    @Test(expected = AlertException.class)
    public void testParsePid() throws Exception
    {
        oozieAlertListener.parsePid("test");
    }

    @Test
    public void testNotifyUser() throws Exception
    {
        oozieAlertListener.notifyUser();
    }

    @Test
    public void testGetSubscriberId() throws Exception
    {
        oozieAlertListener.getSubscriberId();
    }

    @Test(expected = AlertException.class)
    public void testOnAlertEnvironmentIsNull() throws Exception
    {
        List<OozieClusterConfig> myList = new ArrayList<>();
        myList.add(oozieClusterConfig);
        when(oozieImpl.getClusters()).thenReturn(myList);
        when(oozieClusterConfig.getEnvironmentId()).thenReturn(uuid);
        when(containerHostMetric.getEnvironmentId()).thenReturn(uuid);
        when(oozieImpl.getEnvironmentManager()).thenReturn(environmentManager);
        when(environmentManager.findEnvironment(any(UUID.class))).thenReturn(null);


        oozieAlertListener.onAlert(containerHostMetric);

    }

    @Test
    public void testOnAlertNotBelongToOozie() throws Exception
    {
        List<OozieClusterConfig> myList = new ArrayList<>();
        myList.add(oozieClusterConfig);
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add(containerHost);
        when(oozieImpl.getClusters()).thenReturn(myList);
        when(oozieClusterConfig.getEnvironmentId()).thenReturn(uuid);
        when(containerHostMetric.getEnvironmentId()).thenReturn(uuid);
        when(oozieImpl.getEnvironmentManager()).thenReturn(environmentManager);
        when(environmentManager.findEnvironment(any(UUID.class))).thenReturn(environment);
        when(environment.getContainerHosts()).thenReturn(mySet);
        when(containerHost.getId()).thenReturn(uuid);
        when(containerHostMetric.getHostId()).thenReturn(uuid);

        oozieAlertListener.onAlert(containerHostMetric);

    }

    @Test(expected = AlertException.class)
    public void testOnAlert() throws Exception
    {
        List<OozieClusterConfig> myList = new ArrayList<>();
        myList.add(oozieClusterConfig);
        Set<ContainerHost> mySet = new HashSet<>();
        mySet.add(containerHost);
        Set<UUID> myUUID = new HashSet<>();
        myUUID.add(uuid);
        when(oozieImpl.getClusters()).thenReturn(myList);
        when(oozieClusterConfig.getEnvironmentId()).thenReturn(uuid);
        when(containerHostMetric.getEnvironmentId()).thenReturn(uuid);
        when(oozieImpl.getEnvironmentManager()).thenReturn(environmentManager);
        when(environmentManager.findEnvironment(any(UUID.class))).thenReturn(environment);
        when(environment.getContainerHosts()).thenReturn(mySet);
        when(containerHost.getId()).thenReturn(uuid);
        when(containerHostMetric.getHostId()).thenReturn(uuid);
        when(oozieClusterConfig.getAllNodes()).thenReturn(myUUID);
        when(containerHost.execute(any(RequestBuilder.class))).thenReturn(commandResult);
        when(commandResult.hasSucceeded()).thenReturn(true);
        when(commandResult.getStdOut()).thenReturn("12345");
        when((oozieImpl.getAlertSettings())).thenReturn(monitoringSettings);
        when(containerHost.getProcessResourceUsage(12345)).thenReturn(processResourceUsage);
        when(oozieClusterConfig.isAutoScaling()).thenReturn(true);
        when(oozieImpl.getHadoopManager()).thenReturn(hadoop);
        when(hadoop.getCluster(anyString())).thenReturn(hadoopClusterConfig);

        oozieAlertListener.onAlert(containerHostMetric);

    }

}