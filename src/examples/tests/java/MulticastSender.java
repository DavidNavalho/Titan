package examples.tests.java;

import java.net.*;
import java.io.*;
import java.util.*;

public class MulticastSender {

    public static void main(String[] args ) throws Exception {
	    if( args.length != 3 ) {
		System.err.println("usage: java MulticastSender  grupo_multicast porto time-interval") ;
		System.exit(0) ;
	    }
 
    int moreQuotes=20; // change if needed

    int port = Integer.parseInt( args[1]) ;
    InetAddress group = InetAddress.getByName( args[0] ) ;
    int timeinterval = Integer.parseInt( args[2]) ;
    String msg;

    if( !group.isMulticastAddress() ) {
	System.err.println("Multicast address required...") ;
	System.exit(0) ;
    }

    MulticastSocket ms = new MulticastSocket() ;
    do {
        msg = new Date().toString();
	ms.send( new DatagramPacket( msg.getBytes(), msg.getBytes().length, group, port ) ) ;
	--moreQuotes;

	try {
	    Thread.sleep(1000*timeinterval);
	} 
	catch (InterruptedException e) { }

    } while( moreQuotes >0 ) ;
    msg="fim";
    ms.send( new DatagramPacket( msg.getBytes(), msg.getBytes().length, group, port ) ) ;
    ms.close();
	    
    }
}


