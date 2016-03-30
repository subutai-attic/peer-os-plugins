/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.accumulo.ui.wizard;


import io.subutai.common.util.FileUtil;
import io.subutai.plugin.accumulo.api.SetupType;
import io.subutai.plugin.accumulo.ui.AccumuloPortalModule;

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

        Label welcomeMsg = new Label( "<center><h2>Welcome to Accumulo Installation Wizard!</h2>" );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        // Image as a file resource
        logoImg.setIcon( new FileResource( FileUtil.getFile( AccumuloPortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 56, Unit.PIXELS );
        logoImg.setWidth( 220, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        Button startOverHadoopNZK = new Button( "Start over Hadoop & ZK installation" );
        startOverHadoopNZK.setId( "startOverHadoopNZK" );
        startOverHadoopNZK.addStyleName( "default" );
        grid.addComponent( startOverHadoopNZK, 4, 4, 4, 4 );
        grid.setComponentAlignment( startOverHadoopNZK, Alignment.BOTTOM_RIGHT );

        startOverHadoopNZK.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                wizard.init();
                wizard.getConfig().setSetupType( SetupType.OVER_HADOOP_N_ZK );
                wizard.next();
            }
        } );

        setContent( grid );
    }
}
