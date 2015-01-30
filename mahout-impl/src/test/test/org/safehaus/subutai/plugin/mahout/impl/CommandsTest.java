package org.safehaus.subutai.plugin.mahout.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.common.command.OutputRedirection;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

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
                new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 360 )
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