package org.safehaus.subutai.plugin.oozie.impl;


import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OverHadoopSetupStrategy extends OozieSetupStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger(OverHadoopSetupStrategy.class.getName());
    private Environment environment;

    public OverHadoopSetupStrategy( OozieImpl manager, TrackerOperation po, OozieClusterConfig config )
    {
        super( manager, po, config );
        this.environment = environment;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();
        configure();
        return config;
    }

    private void configure() throws ClusterSetupException {
//        po.addLog( "Updating db..." );
//        //save to db
//        config.setEnvironmentId( environment.getId() );
//        manager.getPluginDao().saveInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
//        po.addLog( "Cluster info saved to DB\nInstalling Flume..." );
//        //install pig,
//        String s = Commands.make( CommandType.INSTALL_CLIENT );
//        //RequestBuilder installCommand = new RequestBuilder( s ).withTimeout( 1800 );
//        for ( ContainerHost node : environment.getContainerHostsByIds( config.getNodes() ) )
//        {
//            try
//            {
//                CommandResult result = node.execute(new RequestBuilder( s ).withTimeout( 600 ));
//                processResult( node, result );
//
//            }
//            catch ( CommandException e )
//            {
//                throw new ClusterSetupException(
//                        String.format( "Error while installing Flume on container %s; %s", node.getHostname(),
//                                e.getMessage() ) );
//            }
//        }
//
//        po.addLog( "Configuring cluster..." );
    }

    private void check() throws ClusterSetupException {
//        HadoopClusterConfig hc = manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
//        if ( hc == null )
//        {
//            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
//        }
//        if ( !hc.getAllNodes().containsAll( config.getNodes() ) )
//        {
//            throw new ClusterSetupException(
//                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
//        }
//
//        po.addLog("Checking prerequisites...");
//
//        RequestBuilder checkInstalledCommand = new RequestBuilder( Commands.make( CommandType.STATUS ) );
//        for( UUID uuid : config.getNodes())
//        {
//            ContainerHost node = environment.getContainerHostById( uuid );
//            try
//            {
//                CommandResult result = node.execute( checkInstalledCommand );
//                if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
//                {
//                    po.addLog(
//                            String.format( "Node %s already has Flume installed. Omitting this node from installation",
//                                    node.getHostname() ) );
//                    config.getNodes().remove( node.getId() );
//                }
//                /*else if ( !result.getStdOut()
//                        .contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase() ) )
//                {
//                    po.addLog(
//                            String.format( "Node %s has no Hadoop installation. Omitting this node from installation",
//                                    node.getHostname() ) );
//                    config.getNodes().remove( node.getId() );
//                }*/
//            }
//            catch ( CommandException e )
//            {
//                throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
//            }
//        }
//        if ( config.getNodes().isEmpty() )
//        {
//            throw new ClusterSetupException( "No nodes eligible for installation. Operation aborted" );
//        }
    }
    public void processResult( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {

        if ( !result.hasSucceeded() )
        {
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }

        //check if node agents are connected
        /*for ( Agent agent : config.getClients() )
        {
            String hostname = agent.getHostname();
            if ( oozieManager.getAgentManager().getAgentByHostname( hostname ) == null )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", hostname ) );
            }
        }
        String serverHostName = config.getServer().getHostname();
        if ( oozieManager.getAgentManager().getAgentByHostname( serverHostName ) == null )
        {
            throw new ClusterSetupException( String.format( "Node %s is not connected", serverHostName ) );
        }

        HadoopClusterConfig hc = oozieManager.getHadoopManager().getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }


        Set<Agent> allOozieAgents = config.getAllOozieAgents();

        if ( !hc.getAllNodes().containsAll( allOozieAgents ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }

        Command cmd = oozieManager.getCommandRunner().createCommand(
                new RequestBuilder( oozieManager.getCommands().make( CommandType.STATUS ) ), allOozieAgents );
        oozieManager.getCommandRunner().runCommand( cmd );
        if ( !cmd.hasSucceeded() )
        {
            throw new ClusterSetupException( "Failed to check installed packages" );
        }

        po.addLog( String.format( "Installing Oozie server on %s...", config.getServer().getHostname() ) );
        String sserver = Commands.make( CommandType.INSTALL_SERVER );
        Agent serverAgent = config.getServer();
        cmd = oozieManager.getCommandRunner().createCommand( new RequestBuilder( sserver ).withTimeout( 1800 ),
                Sets.newHashSet( serverAgent ) );
        oozieManager.getCommandRunner().runCommand( cmd );

        if ( cmd.hasSucceeded() )
        {
            po.addLog( "Installation of server succeeded" );
        }
        else
        {
            throw new ClusterSetupException( "Installation failed: " + cmd.getAllErrors() );
        }


        if ( !config.getClients().isEmpty() )
        {
            po.addLog( "Installing Oozie client ..." );
            String sclient = Commands.make( CommandType.INSTALL_CLIENT );

            Set<Agent> clients = config.getClients();
            cmd = oozieManager.getCommandRunner()
                              .createCommand( new RequestBuilder( sclient ).withTimeout( 1800 ), clients );
            oozieManager.getCommandRunner().runCommand( cmd );

            if ( cmd.hasSucceeded() )
            {
                po.addLog( "Installation of clients succeeded" );
            }
            else
            {
                throw new ClusterSetupException( "Installation of clients failed: " + cmd.getAllErrors() );
            }
        }
        else
        {
            po.addLog( "No client is selected, continuing" );
        }

        po.addLog( "Saving to db..." );
        oozieManager.getPluginDAO().saveInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        po.addLog( "Cluster info successfully saved" );
*/
//        return config;
    }

