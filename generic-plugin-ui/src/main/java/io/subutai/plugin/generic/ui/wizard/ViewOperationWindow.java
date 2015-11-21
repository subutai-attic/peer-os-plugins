package io.subutai.plugin.generic.ui.wizard;


import org.apache.commons.codec.binary.Base64;

import com.vaadin.ui.CheckBox;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

import io.subutai.plugin.generic.api.model.Operation;


/**
 * Created by ermek on 11/21/15.
 */
public class ViewOperationWindow extends Window
{
    public ViewOperationWindow( Operation operation )
    {
        super("Operation info");
        setModal( true );
        setClosable( true );
        addStyleName( "default" );
        center();
        setHeight( "50%" );
        setWidth( "50%" );

        VerticalLayout subContent = new VerticalLayout();
        subContent.setSpacing( true );
        subContent.setMargin( true );

        VerticalLayout textItems = new VerticalLayout();
        textItems.setSpacing( true );
        textItems.setMargin( true );

        TextField name = new TextField( "Operation name:" );
        TextField cwd = new TextField( "CWD:" );
        TextField timeOut = new TextField( "Timeout:" );
        CheckBox daemon = new CheckBox( "Daemon:" );

        TextArea scriptArea = new TextArea( "Command:" );
        scriptArea.setSizeFull();

        byte[] decodedBytes = Base64.decodeBase64( operation.getCommandName() );

        name.setValue( operation.getOperationName() );
        cwd.setValue( operation.getCwd() );
        timeOut.setValue( operation.getTimeout() );
        daemon.setValue( operation.getDaemon() );
        scriptArea.setValue( new String( decodedBytes ) );

        name.setReadOnly( true );
        cwd.setReadOnly( true );
        timeOut.setReadOnly( true );
        daemon.setReadOnly( true );
        scriptArea.setReadOnly( true );

        textItems.addComponent( name );
        textItems.addComponent( cwd );
        textItems.addComponent( timeOut );
        textItems.addComponent( daemon );
        subContent.addComponent( textItems );
        subContent.addComponent( scriptArea );

        subContent.setSizeFull();
        subContent.setExpandRatio( scriptArea, 1f );
        setContent( subContent );
    }
}
