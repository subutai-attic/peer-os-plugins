package org.safehaus.subutai.plugin.storm.ui.wizard;


import org.safehaus.subutai.common.util.FileUtil;
import org.safehaus.subutai.plugin.storm.ui.StormPortalModule;

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

        Label welcomeMsg = new Label( "<center><h2>Welcome to Storm Installation Wizard!</h2>" );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        logoImg.setIcon( new FileResource( FileUtil.getFile( StormPortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 150, Unit.PIXELS );
        logoImg.setWidth( 150, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        // Button startEmbeddedZK = new Button( "Start (embedded Zookeeper)" );
        Button startEmbeddedZK = new Button( "Start" );
        startEmbeddedZK.setId( "StormStartEmbedded" );
        startEmbeddedZK.addStyleName( "default" );
        grid.addComponent( startEmbeddedZK, 6, 4, 6, 4 );
        grid.setComponentAlignment( startEmbeddedZK, Alignment.BOTTOM_RIGHT );

        /**
         * Since containers in different environments cannot talk to each other
         * due to the network isolation between environments, for now we need to
         * disable external zookeeper installation type of storm plugin.
         */
        Button startExternalZK = new Button( "Start (external Zookeeper)" );
        startExternalZK.setId( "StormExternal" );
        startExternalZK.addStyleName( "default" );
        // grid.addComponent( startExternalZK, 7, 4, 7, 4 );
        // grid.setComponentAlignment( startExternalZK, Alignment.BOTTOM_RIGHT );

        startEmbeddedZK.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.init();
                wizard.getConfig().setExternalZookeeper( false );
                wizard.next();
            }
        } );

        startExternalZK.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.init();
                wizard.getConfig().setExternalZookeeper( true );
                wizard.next();
            }
        } );
        setContent( grid );
    }
}
