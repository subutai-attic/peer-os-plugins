package io.subutai.plugin.oozie.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.oozie.api.OozieClusterConfig;

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
    public void testGetStartServerCommand() throws Exception
    {
        RequestBuilder command = Commands.getStartServerCommand();

        assertNotNull( command );
    }


    @Test
    public void testGetStopServerCommand() throws Exception
    {
        RequestBuilder command = Commands.getStopServerCommand();

        assertNotNull( command );
    }


    @Test
    public void testGetStatusServerCommand() throws Exception
    {
        RequestBuilder command = Commands.getStatusServerCommand();

        assertNotNull( command );
        assertEquals( new RequestBuilder( "jps" ), command );
    }


    @Test
    public void testGetConfigureRootHostsCommand() throws Exception
    {
        RequestBuilder command = Commands.getConfigureRootHostsCommand();

        assertNotNull( command );
    }


    @Test
    public void testGetConfigureRootGroupsCommand() throws Exception
    {
        RequestBuilder command = Commands.getConfigureRootGroupsCommand();

        assertNotNull( command );
    }


    @Test
    public void testGetUninstallServerCommand() throws Exception
    {
        RequestBuilder command = Commands.getUninstallServerCommand();

        assertNotNull( command );
        assertEquals( new RequestBuilder( "apt-get --force-yes --assume-yes purge " + Commands.SERVER_PACKAGE_NAME )
                .withTimeout( 90 ).withStdOutRedirection( OutputRedirection.NO ), command );
    }
}