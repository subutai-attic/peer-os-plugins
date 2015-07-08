package io.subutai.plugin.flume.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import io.subutai.common.settings.Common;
import io.subutai.plugin.flume.impl.CommandType;
import io.subutai.plugin.flume.impl.Commands;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CommandsTest
{
    private Commands commands;
    @Before
    public void setUp() throws Exception
    {
        commands = new Commands();
    }

    @Test
    public void testMakeCommandTypeStatus() throws Exception
    {
        commands.make( CommandType.STATUS);

        // assertions
        assertEquals("dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH,commands.make(CommandType.STATUS));
    }

    @Test
    public void testMakeCommandTypePurge()
    {
        commands.make(CommandType.PURGE);

        // assertions
        assertEquals("apt-get --force-yes --assume-yes purge subutai-flume",commands.make(CommandType.PURGE));
    }

    @Test
    public void testMakeCommandTypeStop()
    {
        commands.make(CommandType.STOP);

        // assertions
        assertEquals("service flume-ng stop agent", commands.make(CommandType.STOP));
    }

    @Test
    public void testMakeCommandTypeStart()
    {
        commands.make(CommandType.START);

        // assertions
        assertEquals("service flume-ng start agent &",commands.make(CommandType.START));
    }

    @Test
    public void testMakeCommandTypeServiceStatus()
    {
        commands.make(CommandType.SERVICE_STATUS);

        // assertions
        assertEquals("ps axu | grep [f]lume",commands.make(CommandType.SERVICE_STATUS));
    }
}