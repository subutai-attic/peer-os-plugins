package io.subutai.plugin.oozie.ui.wizard;


import com.vaadin.server.FileResource;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;

import io.subutai.common.util.FileUtil;
import io.subutai.plugin.oozie.api.SetupType;
import io.subutai.plugin.oozie.ui.OoziePortalModule;


public class StepStart extends Panel
{

    public StepStart( final Wizard wizard )
    {

        setSizeFull();

        GridLayout grid = new GridLayout( 10, 6 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label welcomeMsg = new Label( "<center><h2>Welcome to Oozie Installation Wizard!</h2>" );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        // Image as a file resource
        logoImg.setIcon( new FileResource( FileUtil.getFile( OoziePortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 56, Unit.PIXELS );
        logoImg.setWidth( 220, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        Button startOverHadoopNZK = new Button( "Start over Hadoop installation" );
        startOverHadoopNZK.setId( "OozieStartOverHadoop" );
        startOverHadoopNZK.addStyleName( "default" );
        grid.addComponent( startOverHadoopNZK, 7, 4, 7, 4 );
        grid.setComponentAlignment( startOverHadoopNZK, Alignment.BOTTOM_LEFT );

        startOverHadoopNZK.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                wizard.init();
                wizard.getConfig().setSetupType( SetupType.OVER_HADOOP );
                wizard.next();
            }
        } );

        setContent( grid );
    }
}
