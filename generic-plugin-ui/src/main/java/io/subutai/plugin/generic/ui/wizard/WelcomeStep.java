package io.subutai.plugin.generic.ui.wizard;


import com.vaadin.annotations.Theme;
import com.vaadin.server.FileResource;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Panel;

import io.subutai.common.util.FileUtil;
import io.subutai.plugin.generic.api.Profile;
import io.subutai.plugin.generic.ui.GenericPluginPortalModule;


@Theme( "valo" )
public class WelcomeStep extends Panel
{
    public WelcomeStep( final Wizard wizard )
    {
        setSizeFull();

        GridLayout grid = new GridLayout( 10, 6 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label welcomeMsg =
                new Label( "<center><h2>Welcome to Generic Plugin, execute your custom commands on containers </h2>" );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        logoImg.setIcon( new FileResource( FileUtil.getFile( GenericPluginPortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 150, Unit.PIXELS );
        logoImg.setWidth( 150, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        HorizontalLayout buttonsGrid = new HorizontalLayout();
        final Button createProfile = new Button( "Create profile" );
        createProfile.addStyleName( "default" );
        final Button configureOperations = new Button( "Configure operations" );
        configureOperations.addStyleName( "default" );
        final Button manageContainers = new Button( "Manage containers" );
        manageContainers.addStyleName( "default" );
        buttonsGrid.addComponent( createProfile );
        buttonsGrid.addComponent( configureOperations );
        buttonsGrid.addComponent( manageContainers );
        grid.addComponent( buttonsGrid, 6, 4, 6, 4 );
        grid.setComponentAlignment( buttonsGrid, Alignment.BOTTOM_RIGHT );

        createProfile.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.changeWindow( 1 );
                wizard.putForm();
            }
        } );

        configureOperations.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                //            	if (wizard.getConfig().getProfiles().isEmpty())
                //				{
                //					Notification notif = new Notification ("Please create profiles first");
                //					notif.setDelayMsec(2000);
                //					notif.show(Page.getCurrent());
                //				}
                //				else
                //				{
                wizard.changeWindow( 2 );
                wizard.putForm();
                //				}
            }
        } );

        manageContainers.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                boolean exist = false;
                for ( Profile p : wizard.getConfig().getProfiles() )
                {
                    if ( !p.getOperations().isEmpty() )
                    {
                        exist = true;
                        break;
                    }
                }
                    wizard.changeWindow( 3 );
                    wizard.putForm();
            }
        } );

        setContent( grid );
    }
}

