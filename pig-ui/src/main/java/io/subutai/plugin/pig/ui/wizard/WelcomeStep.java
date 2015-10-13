package io.subutai.plugin.pig.ui.wizard;


import com.vaadin.server.FileResource;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;

import io.subutai.common.util.FileUtil;
import io.subutai.plugin.pig.ui.PigPortalModule;


public class WelcomeStep extends Panel
{

    public WelcomeStep( final Wizard wizard )
    {

        setSizeFull();

        GridLayout grid = new GridLayout( 10, 6 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label welcomeMsg = new Label( "<center><h2>Welcome to Pig Installation Wizard!</h2>" );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        logoImg.setIcon( new FileResource( FileUtil.getFile( PigPortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 200, Unit.PIXELS );
        logoImg.setWidth( 180, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        Button next = new Button( "Start installation" );
        next.setId( "PigStartOverHadoop" );
        next.addStyleName( "default" );
        //		next.setWidth(100, Unit.PIXELS);
        grid.addComponent( next, 4, 4, 4, 4 );
        grid.setComponentAlignment( next, Alignment.BOTTOM_RIGHT );

        next.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent clickEvent )
            {
                clickHandler( wizard );
            }
        } );

        setContent( grid );
    }


    private void clickHandler( Wizard wizard )
    {
        wizard.init();
        wizard.next();
    }
}
