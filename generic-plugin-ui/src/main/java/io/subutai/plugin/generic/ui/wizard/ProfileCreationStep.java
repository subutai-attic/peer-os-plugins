package io.subutai.plugin.generic.ui.wizard;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gwt.thirdparty.guava.common.base.Strings;
import com.vaadin.server.Page;
import com.vaadin.ui.Button;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.Table;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.model.Profile;


public class ProfileCreationStep extends Panel
{
    private static final String CONFIGURE_BUTTON_CAPTION = "Configure Operations";
    private static final String DELETE_BUTTON_CAPTION = "Delete";
    private GenericPlugin genericPlugin;
    private Wizard wizard;
    private static final Logger LOG = LoggerFactory.getLogger( ProfileCreationStep.class.getName() );
    private Panel panel;
    private VerticalLayout panelContent;
    private TextField newProfile;
    private HorizontalLayout buttonsGrid;
    private Button create;
    private Button back;
    private Table profileTable;


    public ProfileCreationStep( final Wizard wizard, final GenericPlugin genericPlugin )
    {
        this.setSizeFull();
        this.wizard = wizard;
        this.genericPlugin = genericPlugin;

        panel = new Panel( "<h2>Create profile<h2>" );
        panelContent = new VerticalLayout();
        panel.setContent( panelContent );
        panelContent.setSpacing( true );

        newProfile = new TextField( "Profile" );
        newProfile.setInputPrompt( "New profile name" );
        newProfile.setRequired( true );
        panelContent.addComponent( newProfile );

        buttonsGrid = new HorizontalLayout();
        buttonsGrid.setSpacing( true );

        create = new Button( "Create" );
        create.addStyleName( "default" );

        back = new Button( "Back" );
        back.addStyleName( "default" );

        profileTable = new Table( "Profiles" );
        profileTable.addContainerProperty( "Profile name", String.class, null );
        //        profileTable.addContainerProperty( "Action", Button.class, null );
        profileTable.addContainerProperty( "Actions", HorizontalLayout.class, null );
        profileTable.setSizeFull();
        profileTable.setPageLength( 10 );
        profileTable.setSelectable( false );
        profileTable.setImmediate( true );


        buttonsGrid.addComponent( back );
        buttonsGrid.addComponent( create );


        panelContent.addComponent( buttonsGrid );
        panelContent.addComponent( profileTable );

        populateTable();

        create.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( newProfile.getValue() ) )
                {
                    show( "Please enter profile name" );
                }
                else
                {
                    boolean exists = false;
                    for ( Profile p : genericPlugin.getProfiles() )
                    {
                        LOG.debug( p.getName() );
                        LOG.debug( newProfile.getValue() );
                        if ( p.getName().equals( newProfile.getValue() ) )
                        {
                            exists = true;
                            break;
                        }
                    }
                    if ( exists )
                    {
                        show( "Profile already exists" );
                    }
                    else
                    {
                        genericPlugin.saveProfile( newProfile.getValue() );
                        populateTable();
                        //                        wizard.changeWindow( 0 );
                        //                        wizard.putForm();
                        show( "Profile successfully created" );
                    }
                }
            }
        } );

        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.changeWindow( 0 );
                wizard.putForm();
            }
        } );

        this.setContent( panel );
    }


    private void populateTable()
    {
        profileTable.removeAllItems();

        List<Profile> profiles = genericPlugin.getProfiles();

        if ( profiles != null && !profiles.isEmpty() )
        {
            for ( final Profile profile : genericPlugin.getProfiles() )
            {
                final Button configureBtn = new Button( CONFIGURE_BUTTON_CAPTION );
                configureBtn.addStyleName( "default" );
                final Button deleteBtn = new Button( DELETE_BUTTON_CAPTION );
                deleteBtn.addStyleName( "default" );

                final HorizontalLayout availableOperations = new HorizontalLayout();
                availableOperations.addStyleName( "default" );
                availableOperations.setSpacing( true );

                addGivenComponents( availableOperations, configureBtn, deleteBtn );

                profileTable.addItem( new Object[] {
                        profile.getName(), availableOperations
                }, null );

                addClickListenerToConfigureButton( configureBtn, profile );
                addClickListenerToDeleteButton( deleteBtn, profile );
            }
        }
    }


    private void addClickListenerToConfigureButton( final Button configureBtn, final Profile profile )
    {
        getButton( CONFIGURE_BUTTON_CAPTION, configureBtn ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                wizard.setCurrentProfileId( profile.getId() );
                wizard.changeWindow( 2 );
                wizard.putForm();
            }
        } );
    }


    private void addClickListenerToDeleteButton( final Button deleteBtn, final Profile profile )
    {
        getButton( DELETE_BUTTON_CAPTION, deleteBtn ).addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( Button.ClickEvent event )
            {
                genericPlugin.deleteProfile( profile.getId() );
                genericPlugin.deleteOperations( profile.getId() );
                populateTable();
                show( "Profile deleted successfully" );
            }
        } );
    }


    private Button getButton( String caption, Button... buttons )
    {
        for ( Button b : buttons )
        {
            if ( b.getCaption().equals( caption ) )
            {
                return b;
            }
        }
        return null;
    }


    private void show( String notification )
    {
        Notification notif = new Notification( notification );
        notif.setDelayMsec( 2000 );
        notif.show( Page.getCurrent() );
    }


    private void addGivenComponents( HorizontalLayout layout, Button... buttons )
    {
        for ( Button b : buttons )
        {
            layout.addComponent( b );
        }
    }
}
