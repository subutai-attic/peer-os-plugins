package io.subutai.plugin.hadoop.impl;

import org.junit.Before;
import org.junit.Test;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.Commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class CommandsTest
{
    Commands commands;
    HadoopClusterConfig hadoopClusterConfig;

    @Before
    public void setUp()
    {
        hadoopClusterConfig = mock(HadoopClusterConfig.class);
        commands = new Commands(hadoopClusterConfig);
    }

    @Test
    public void testGetStatusNameNodeCommand()
    {
        assertEquals( "service hadoop-dfs status", Commands.getStatusNameNodeCommand() );
        assertNotNull( Commands.getStatusNameNodeCommand() );
    }

    @Test
    public void testGetStartNameNodeCommand()
    {
        assertEquals( "service hadoop-dfs start", Commands.getStartNameNodeCommand() );
        assertNotNull( Commands.getStartNameNodeCommand() );
    }

    @Test
    public void testGetStopNameNodeCommand()
    {
        assertEquals( "service hadoop-dfs stop", Commands.getStopNameNodeCommand() );
        assertNotNull( Commands.getStopNameNodeCommand() );
    }

    @Test
    public void testGetStartJobTrackerCommand()
    {
        assertEquals( "service hadoop-mapred start", Commands.getStartJobTrackerCommand() );
        assertNotNull( Commands.getStartJobTrackerCommand() );
    }

    @Test
    public void testGetStopJobTrackerCommand()
    {
        assertEquals( "service hadoop-mapred stop", Commands.getStopJobTrackerCommand() );
        assertNotNull( Commands.getStopJobTrackerCommand() );
    }

    @Test
    public void testGetStatusJobTrackerCommand()
    {
        assertEquals( "service hadoop-mapred status", Commands.getStatusJobTrackerCommand() );
        assertNotNull( Commands.getStatusJobTrackerCommand() );
    }

    @Test
    public void testGetStatusDataNodeCommand()
    {
        assertEquals( "service hadoop-dfs status", Commands.getStatusDataNodeCommand() );
        assertNotNull( Commands.getStatusDataNodeCommand() );
    }

    @Test
    public void testGetClearMastersCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh masters clear",
                Commands.getClearMastersCommand() );
        assertNotNull( Commands.getClearMastersCommand() );
    }

    @Test
    public void testGetClearSlavesCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves clear", Commands.getClearSlavesCommand() );
        assertNotNull( Commands.getClearSlavesCommand() );
    }

    @Test
    public void testGetRefreshJobTrackerCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop mradmin -refreshNodes", Commands.getRefreshJobTrackerCommand() );
        assertNotNull( Commands.getRefreshJobTrackerCommand() );
    }

    @Test
    public void testGetStartDataNodeCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop-daemon.sh start datanode", Commands.getStartDataNodeCommand() );
        assertNotNull( Commands.getStartDataNodeCommand() );
    }

    @Test
    public void testGetStopDataNodeCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop-daemon.sh stop datanode", Commands.getStopDataNodeCommand() );
        assertNotNull( Commands.getStopDataNodeCommand() );
    }

    @Test
    public void testGetStartTaskTrackerCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop-daemon.sh start tasktracker", Commands
                .getStartTaskTrackerCommand());
        assertNotNull( Commands.getStartTaskTrackerCommand() );
    }

    @Test
    public void testGetStopTaskTrackerCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop-daemon.sh stop tasktracker",
                Commands.getStopTaskTrackerCommand() );
        assertNotNull( Commands.getStopTaskTrackerCommand() );
    }

    @Test
    public void testGetStatusTaskTrackerCommand()
    {
        assertEquals( "service hadoop-mapred status", Commands.getStatusTaskTrackerCommand() );
        assertNotNull( Commands.getStatusTaskTrackerCommand() );
    }

    @Test
    public void testGetFormatNameNodeCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop namenode -format", Commands.getFormatNameNodeCommand() );
        assertNotNull( Commands.getFormatNameNodeCommand() );
    }

    @Test
    public void testGetReportHadoopCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop dfsadmin -report", Commands.getReportHadoopCommand() );
        assertNotNull( Commands.getReportHadoopCommand() );
    }

    @Test
    public void testGetRefreshNameNodeCommand()
    {
        assertEquals( ". /etc/profile && " + "hadoop dfsadmin -refreshNodes", Commands.getRefreshNameNodeCommand() );
        assertNotNull( Commands.getRefreshNameNodeCommand() );
    }

    @Test
    public void testGetSetDataNodeCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves " + hostname,
                Commands.getSetDataNodeCommand( hostname ) );
        assertNotNull( Commands.getSetDataNodeCommand( hostname ) );
    }

    @Test
    public void testGetExcludeDataNodeCommand()
    {
        String ip = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh dfs.exclude clear " + ip,
                Commands.getExcludeDataNodeCommand( ip ) );
        assertNotNull( Commands.getExcludeDataNodeCommand( ip ) );
    }

    @Test
    public void testGetSetTaskTrackerCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves " + hostname,
                Commands.getSetTaskTrackerCommand( hostname ) );
        assertNotNull( Commands.getSetTaskTrackerCommand( hostname ) );
    }

    @Test
    public void testGetExcludeTaskTrackerCommand()
    {
        String ip = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh mapred.exclude clear " + ip,
                Commands.getExcludeTaskTrackerCommand( ip ) );
        assertNotNull( Commands.getExcludeTaskTrackerCommand( ip ) );
    }

    @Test
    public void testGetRemoveTaskTrackerCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves clear " + hostname,
                Commands.getRemoveTaskTrackerCommand( hostname ) );
        assertNotNull( Commands.getRemoveTaskTrackerCommand( hostname ) );
    }

    @Test
    public void testGetIncludeTaskTrackerCommand()
    {
        String ip = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh mapred.exclude " + ip,
                Commands.getIncludeTaskTrackerCommand( ip ) );
        assertNotNull( Commands.getIncludeTaskTrackerCommand( ip ) );
    }

    @Test
    public void testGetRemoveDataNodeCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves clear " + hostname,
                Commands.getRemoveDataNodeCommand( hostname ) );
        assertNotNull( Commands.getRemoveDataNodeCommand( hostname ) );
    }

    @Test
    public void testGetIncludeDataNodeCommand()
    {
        String ip = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh dfs.exclude " + ip,
                Commands.getIncludeDataNodeCommand( ip ) );
        assertNotNull( Commands.getIncludeDataNodeCommand( ip ) );
    }

    @Test
    public void testGetConfigureJobTrackerCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves " + hostname,
                Commands.getConfigureJobTrackerCommand( hostname ) );
        assertNotNull( Commands.getConfigureJobTrackerCommand( hostname ) );
    }

    @Test
    public void testGetConfigureSecondaryNameNodeCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh masters " + hostname,
                Commands.getConfigureSecondaryNameNodeCommand( hostname ) );
        assertNotNull( Commands.getConfigureSecondaryNameNodeCommand( hostname ) );
    }

    @Test
    public void testGetConfigureDataNodesCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves " + hostname,
                Commands.getConfigureSlaveNodes( hostname ) );
        assertNotNull( Commands.getConfigureSlaveNodes( hostname ) );
    }

    @Test
    public void testGetConfigureTaskTrackersCommand()
    {
        String hostname = "test";
        assertEquals( ". /etc/profile && " + "hadoop-master-slave.sh slaves " + hostname,
                Commands.getConfigureTaskTrackersCommand( hostname ) );
        assertNotNull( Commands.getConfigureTaskTrackersCommand( hostname ) );
    }

    @Test
    public void testGetSetMastersCommand()
    {
        String namenode = "test";
        String jobtracker = "test2";
        assertEquals(". /etc/profile && " + "hadoop-configure.sh " +
                namenode + ":" + HadoopClusterConfig.NAME_NODE_PORT + " " +
                jobtracker + ":" + HadoopClusterConfig.JOB_TRACKER_PORT + " " +
                hadoopClusterConfig.getReplicationFactor(), commands.getSetMastersCommand(namenode, jobtracker));
        assertNotNull(commands.getSetMastersCommand(namenode, jobtracker));

    }
}