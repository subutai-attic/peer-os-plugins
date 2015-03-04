package org.safehaus.subutai.plugin.storm.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith( MockitoJUnitRunner.class )
public class CommandsTest
{
    private Commands commands;


    @Before
    public void setUp() throws Exception
    {
        commands = new Commands();
    }


    @Test
    public void testMake() throws Exception
    {
        commands.make( CommandType.INSTALL );
    }


    @Test
    public void testMakeList() throws Exception
    {
        commands.make( CommandType.LIST, StormService.NIMBUS );
        commands.make( CommandType.KILL, StormService.NIMBUS );
    }


    @Test
    public void testConfigure() throws Exception
    {
        commands.configure( "cmd", "proffile", "property", "value" );
    }
}