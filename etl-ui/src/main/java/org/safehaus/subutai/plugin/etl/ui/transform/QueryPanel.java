package org.safehaus.subutai.plugin.etl.ui.transform;

import com.vaadin.ui.Embedded;
import com.vaadin.ui.Layout;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.Upload;
import com.vaadin.ui.VerticalLayout;


public class QueryPanel extends VerticalLayout
{

    public QueryPanel( )
    {
        init();
    }

    public void init(){
        // Show uploaded file in this placeholder
        final Embedded image = new Embedded("Uploaded Query File");

        image.setVisible(false);

        final QueryFileUploader receiver = new QueryFileUploader( image );

        final TextArea contentOfQueryFile = new TextArea( "Content of uploaded query file:" );
        contentOfQueryFile.setWidth( "600px" );
        contentOfQueryFile.setHeight( "400px" );

        // Create the upload with a caption and set receiver later
        Upload upload = new Upload("Upload query file here ", receiver);
        upload.setButtonCaption("Start Upload");
        upload.setStyleName( "default" );
        upload.addSucceededListener(receiver);
        upload.addSucceededListener( new Upload.SucceededListener()
        {
            @Override
            public void uploadSucceeded( final Upload.SucceededEvent succeededEvent )
            {
                receiver.showUploadedText( contentOfQueryFile, receiver.getFile() );
            }
        } );

        // Put the components in a panel
        Panel panel = new Panel("Upload Panel");
        Layout panelContent = new VerticalLayout();
        panelContent.addComponents(upload, image);
        panel.setContent(panelContent);

        addComponent( panel );
        addComponent( contentOfQueryFile );
    }

}
