package org.safehaus.subutai.plugin.etl.ui.transform;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.vaadin.server.FileResource;
import com.vaadin.server.Page;
import com.vaadin.ui.Embedded;
import com.vaadin.ui.Notification;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.Upload;


public class QueryFileUploader implements Upload.Receiver, Upload.SucceededListener
{
    public File file;
    final Embedded queryFile;
    private static final String UPLOAD_PATH = "/tmp/uploads/";

    public QueryFileUploader( Embedded queryFile ){
        this.queryFile = queryFile;
    }

    @Override
    public OutputStream receiveUpload( String filename, String mimeType )
    {
        // Create upload stream
        FileOutputStream fos = null; // Stream to write to
        try {
            // Open the file for writing.
            file = new File( UPLOAD_PATH + filename);
            fos = new FileOutputStream(file);
        } catch (final java.io.FileNotFoundException e) {
            new Notification("Could not open file<br/>",
                    e.getMessage(),
                    Notification.Type.ERROR_MESSAGE)
                    .show( Page.getCurrent());
            return null;
        }
        return fos; // Return the output stream to write to
    }


    public void showUploadedText( TextArea textArea, File file ){
        if ( file == null ){
            return;
        }
        String content = "";
        try
        {
            content = new String( readFile( file.toPath(), Charset.defaultCharset() ) );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        textArea.setValue( content );
    }


    public void saveChanges( TextArea area, File file ){
        if ( file == null ){
            return;
        }
        String content = area.getValue();
        try
        {
            PrintWriter out = new PrintWriter( file );
            out.print( "" );
            out.print( content );
            out.close();
            showUploadedText( area, file );
        }
        catch ( FileNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    public File createNewFile( String fileName ){
        String content = "";
        File file = new File("/tmp/uploads/" + fileName);
        try
        {
            PrintWriter out = new PrintWriter( file );
            out.print( "This is a sample query file." );
            out.close();
        }
        catch ( FileNotFoundException e )
        {
            e.printStackTrace();
        }
        return file;
    }


    public File getFile(){
        return file;
    }


    public void setFile( File file ){
        this.file = file;
    }


    public String readFile( Path path, Charset encoding ) throws IOException {

        byte[] encoded = Files.readAllBytes( Paths.get( path.toString() ));
        return new String( encoded, encoding );
    }


    public void uploadSucceeded(Upload.SucceededEvent event) {
        // Show the uploaded file in the queryFile viewer
        queryFile.setVisible( true );
        queryFile.setCaption( "File uploaded to " + file.getAbsolutePath() );
        queryFile.setSource( new FileResource( file ) );
    }
}
