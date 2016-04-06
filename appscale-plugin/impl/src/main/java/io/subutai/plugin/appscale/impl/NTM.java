/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class NTM implements X509TrustManager
{
    @Override
    public void checkClientTrusted ( X509Certificate[] xcs, String string ) throws CertificateException
    {

    }


    @Override
    public void checkServerTrusted ( X509Certificate[] xcs, String string ) throws CertificateException
    {

    }


    @Override
    public X509Certificate[] getAcceptedIssuers ()
    {
        return null;
    }

}

