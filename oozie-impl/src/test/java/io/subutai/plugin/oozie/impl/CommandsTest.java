package io.subutai.plugin.oozie.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.CommandType;
import io.subutai.plugin.oozie.impl.Commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@RunWith( MockitoJUnitRunner.class )
public class CommandsTest
{
    public static final String SERVER_PACKAGE_NAME =
            Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_KEY.toLowerCase() + "-server";
    public static final String CLIENT_PACKAGE_NAME =
            Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_KEY.toLowerCase() + "-client";


    private Commands commands;


    @Before
    public void setUp() throws Exception
    {
        commands = new Commands();
    }


    @Test
    public void testMake() throws Exception
    {

    }


    @Test
    public void testMakeCommandTypeStatus() throws Exception
    {
        commands.make( CommandType.STATUS );
        commands.make( CommandType.INSTALL_CLIENT );
        commands.make( CommandType.INSTALL_SERVER );
        commands.make( CommandType.PURGE );
        commands.make( CommandType.START );
        commands.make( CommandType.STOP );
    }

    @Test
    public void testGetStartServerCommand() throws Exception
    {
        RequestBuilder command = commands.getStartServerCommand();

        assertNotNull( command );
        //assertEquals( new RequestBuilder( "service oozie-server start &" ), command );
    }


    @Test
    public void testGetStopServerCommand() throws Exception
    {
        RequestBuilder command = commands.getStopServerCommand();

        assertNotNull( command );
        assertEquals( new RequestBuilder( "service oozie-server stop" ), command );
    }


    @Test
    public void testGetStatusServerCommand() throws Exception
    {
        RequestBuilder command = commands.getStatusServerCommand();

        assertNotNull( command );
        assertEquals( new RequestBuilder( "service oozie-server status" ), command );
    }


    @Test
    public void testGetConfigureRootHostsCommand() throws Exception
    {
        RequestBuilder command = commands.getConfigureRootHostsCommand( "test" );

        assertNotNull( command );
        assertEquals( new RequestBuilder( String.format(
                ". /etc/profile && $HADOOP_HOME/bin/hadoop-property.sh add core-site.xml hadoop.proxyuser"
                        + ".root.hosts %s", "test" ) ), command );
    }


    @Test
    public void testGetConfigureRootGroupsCommand() throws Exception
    {
        RequestBuilder command = commands.getConfigureRootGroupsCommand();

        assertNotNull( command );
        assertEquals( new RequestBuilder( String.format(
                ". /etc/profile && $HADOOP_HOME/bin/hadoop-property.sh add core-site.xml hadoop.proxyuser"
                        + ".root.groups '\\*' " ) ), command );
    }


    @Test
    public void testGetUninstallServerCommand() throws Exception
    {
        RequestBuilder command = commands.getUninstallServerCommand();

        assertNotNull( command );
        assertEquals(
                new RequestBuilder( "apt-get --force-yes --assume-yes purge " + SERVER_PACKAGE_NAME ).withTimeout( 90 )
                                                                                                     .withStdOutRedirection(
                                                                                                             OutputRedirection.NO ),
                command );
    }


    @Test
    public void testGetUninstallClientsCommand() throws Exception
    {
        RequestBuilder command = commands.getUninstallClientsCommand();

        assertNotNull( command );
        assertEquals(
                new RequestBuilder( "apt-get --force-yes --assume-yes purge " + CLIENT_PACKAGE_NAME ).withTimeout( 90 )
                                                                                                     .withStdOutRedirection(
                                                                                                             OutputRedirection.NO ),
                command );
    }
}