package io.subutai.plugin.mahout.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@RunWith( MockitoJUnitRunner.class )
public class CommandsTest
{
    private Commands commands;
    public static final String PACKAGE_NAME = "subutai-spark2";


    @Before
    public void setUp() throws Exception
    {
        commands = new Commands();
    }


    @Test
    public void testGetInstallCommand() throws Exception
    {
        RequestBuilder requestBuilder = commands.getInstallCommand();

        // assertions
        assertNotNull( commands.getInstallCommand() );
    }


    @Test
    public void testGetUninstallCommand() throws Exception
    {
        RequestBuilder requestBuilder = commands.getUninstallCommand();

        // assertions
        assertNotNull( commands.getInstallCommand() );
    }


    @Test
    public void testGetCheckInstalledCommand() throws Exception
    {
        RequestBuilder requestBuilder = commands.getCheckInstalledCommand();


        // assertions
        assertNotNull( commands.getInstallCommand() );
    }
}