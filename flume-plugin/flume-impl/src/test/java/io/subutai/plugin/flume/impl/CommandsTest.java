package io.subutai.plugin.flume.impl;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.subutai.common.settings.Common;

import static org.junit.Assert.assertEquals;


@RunWith( MockitoJUnitRunner.class )
public class CommandsTest
{

    @Test
    public void testMakeCommandTypeStatus() throws Exception
    {
        Commands.make( CommandType.STATUS );

        // assertions
        assertEquals( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH,
                Commands.make( CommandType.STATUS ) );
    }


    @Test
    public void testMakeCommandTypePurge()
    {
        Commands.make( CommandType.PURGE );

        // assertions
        assertEquals( "apt-get --force-yes --assume-yes purge subutai-flume", Commands.make( CommandType.PURGE ) );
    }


    @Test
    public void testMakeCommandTypeStop()
    {
        Commands.make( CommandType.STOP );

        // assertions
        assertEquals( "service flume-ng stop agent", Commands.make( CommandType.STOP ) );
    }


    @Test
    public void testMakeCommandTypeStart()
    {
        Commands.make( CommandType.START );

        // assertions
        assertEquals( "service flume-ng start agent &", Commands.make( CommandType.START ) );
    }


    @Test
    public void testMakeCommandTypeServiceStatus()
    {
        Commands.make( CommandType.SERVICE_STATUS );

        // assertions
        assertEquals( "ps axu | grep [f]lume", Commands.make( CommandType.SERVICE_STATUS ) );
    }
}