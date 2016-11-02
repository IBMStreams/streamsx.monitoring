package com.ibm.streamsx.metrics.internal;

import java.security.cert.CertificateException;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

public class JmxTrustManager implements X509TrustManager {
	private static final String ISSUER = "CN=www.ibm.com,OU=SWG,O=IBM,L=Rochester,ST=MN,C=US";

	public void checkServerTrusted(java.security.cert.X509Certificate [] chain, String authType)
			throws CertificateException {
		for (java.security.cert.X509Certificate cert : chain) {
			X500Principal issuer = cert.getIssuerX500Principal();
			if (!issuer.getName().equals(ISSUER)) {
				throw new CertificateException("invalid issuer=" + issuer.getName());
			}            
		}
	}

	public void checkClientTrusted(java.security.cert.X509Certificate [] chain, String authType)
			throws CertificateException {        
	}

	public java.security.cert.X509Certificate [] getAcceptedIssuers() {
		return null;
	}
}
