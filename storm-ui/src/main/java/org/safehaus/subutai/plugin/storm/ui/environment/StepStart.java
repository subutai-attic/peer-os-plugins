package org.safehaus.subutai.plugin.storm.ui.environment;


import org.safehaus.subutai.common.util.FileUtil;
import org.safehaus.subutai.plugin.storm.ui.StormPortalModule;
import org.safehaus.subutai.plugin.storm.ui.wizard.Wizard;

import com.vaadin.server.FileResource;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;


public class StepStart extends Panel
{

    public StepStart( final EnvironmentWizard environmentWizard )
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

        Button next = new Button( "Start (embedded Zookeeper)" );
        next.setId( "StormStartEmbedded" );
        next.addStyleName( "default" );
        next.addClickListener( new NextClickHandler( environmentWizard, false ) );
        grid.addComponent( next, 6, 4, 6, 4 );
        grid.setComponentAlignment( next, Alignment.BOTTOM_RIGHT );

        Button nextExt = new Button( "Start (external Zookeeper)" );
        nextExt.setId( "StormExternal" );
        nextExt.addStyleName( "default" );
        nextExt.addClickListener( new NextClickHandler( environmentWizard, true ) );
        grid.addComponent( nextExt, 7, 4, 7, 4 );
        grid.setComponentAlignment( nextExt, Alignment.BOTTOM_RIGHT );

        setContent( grid );
    }

    private class NextClickHandler implements Button.ClickListener
    {

        final EnvironmentWizard wizard;
        final boolean withExtZookeeper;


        public NextClickHandler( EnvironmentWizard wizard, boolean withExtZookeeper )
        {
            this.wizard = wizard;
            this.withExtZookeeper = withExtZookeeper;
        }


        @Override
        public void buttonClick( Button.ClickEvent event )
        {
            wizard.init( withExtZookeeper );
            wizard.next();
        }
    }
}
