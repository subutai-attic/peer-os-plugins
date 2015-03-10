package org.safehaus.subutai.plugin.hipi.impl;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;


@RunWith( MockitoJUnitRunner.class )
public class CommandFactoryTest
{
    private CommandFactory commandFactory;


    @Before
    public void setUp() throws Exception
    {
        commandFactory = new CommandFactory();
    }


    @Test
    public void testBuild() throws Exception
    {
        CommandFactory.build( NodeOperationType.CHECK_INSTALLATION );
        CommandFactory.build( NodeOperationType.INSTALL );
        CommandFactory.build( NodeOperationType.UNINSTALL );
    }
}