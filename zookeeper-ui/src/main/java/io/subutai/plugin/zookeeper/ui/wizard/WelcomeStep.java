/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.zookeeper.ui.wizard;


import io.subutai.common.util.FileUtil;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.ui.ZookeeperPortalModule;

import com.vaadin.server.FileResource;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;


public class WelcomeStep extends Panel
{

    public WelcomeStep( final Wizard wizard )
    {

        setSizeFull();

        GridLayout grid = new GridLayout( 10, 6 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label welcomeMsg = new Label( String.format( "<center><h2>Welcome to %s Installation Wizard!</h2>",
                ZookeeperClusterConfig.PRODUCT_NAME ) );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        logoImg.setIcon( new FileResource( FileUtil.getFile( ZookeeperPortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 204, Unit.PIXELS );
        logoImg.setWidth( 150, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        Button startStandaloneOverEnv = new Button( "Start over Environment" );
        startStandaloneOverEnv.setId( "ZookeeperStartStandaloneOverEnv" );
        startStandaloneOverEnv.addStyleName( "default" );
        grid.addComponent( startStandaloneOverEnv, 4, 4, 4, 4 );
        grid.setComponentAlignment( startStandaloneOverEnv, Alignment.BOTTOM_RIGHT );

        Button startOverHadoop = new Button( "Start over-Hadoop installation" );
        startOverHadoop.setId( "ZookeeperStartOverHadoop" );
        startOverHadoop.addStyleName( "default" );
        grid.addComponent( startOverHadoop, 5, 4, 5, 4 );
        grid.setComponentAlignment( startOverHadoop, Alignment.BOTTOM_RIGHT );

        startOverHadoop.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                wizard.init();
                wizard.getConfig().setSetupType( SetupType.OVER_HADOOP );
                wizard.next();
            }
        } );
        startStandaloneOverEnv.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                wizard.init();
                wizard.getConfig().setSetupType( SetupType.OVER_ENVIRONMENT );
                wizard.next();
            }
        } );

        setContent( grid );
    }
}
