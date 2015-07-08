package io.subutai.plugin.mahout.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.plugin.mahout.impl.Commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@RunWith( MockitoJUnitRunner.class )
public class CommandsTest
{
    private Commands commands;
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + MahoutClusterConfig.PRODUCT_KEY.toLowerCase();


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
        assertEquals(
                new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 1000 )
                                                                                                .withStdOutRedirection(
                                                                                                        OutputRedirection.NO ),
                requestBuilder );
    }


    @Test
    public void testGetUninstallCommand() throws Exception
    {
        RequestBuilder requestBuilder = commands.getUninstallCommand();

        // assertions
        assertNotNull( commands.getInstallCommand() );
        assertEquals( new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 60 ),
                requestBuilder );
    }


    @Test
    public void testGetCheckInstalledCommand() throws Exception
    {
        RequestBuilder requestBuilder = commands.getCheckInstalledCommand();


        // assertions
        assertNotNull( commands.getInstallCommand() );
        assertEquals( new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH ),
                requestBuilder );
    }
}