// Copyright (c) 2013, Peter Wood.
// See license.txt for licensing details.
module stalkd.server;

import stalkd.connection;
import stalkd.exceptions;
import stalkd.tube;

/**
 * This class encapsulates the details for a server running the Beanstalkd
 * application and provides a main interaction point for users to obtain
 * Tube objects.
 */
struct Server {
   /**
    * A constant defining the default Beanstalkd port.
    */
   static const DEFAULT_BEANSTALKD_PORT = 11300;

   /**
    * Constructor for the Server class.
    *
    * Params:
    *    host =  The name or IP address of the Beanstalkd server.
    *    port =  The port number that Beanstalkd is listening on. Defaults to
    *            DEFAULT_BEANSTALKD_PORT.
    */
   this(string host, ushort port=DEFAULT_BEANSTALKD_PORT) {
      _host = host;
      _port = port;
   }

   @property string host() {
      return(_host);
   }

   @property ushort port() {
      return(_port);
   }

   /**
    * This method fetches a Tube object representing a named tube on a server
    * running Beanstalkd. This is the preferred method of obtaining a tube as
    * it hides the various class interactions behind a simple interface call.
    *
    * Params:
    *    name =  The name of the tube on the server. Defaults to
    *            Tube.DEFAULT_TUBE_NAME.
    */
   Tube getTube(string name=Tube.DEFAULT_TUBE_NAME) {
      Connection connection = new Connection(this);
      Tube       tube       = new Tube(connection);

      if(name !is Tube.DEFAULT_TUBE_NAME) {
         tube.use(name);
      }
      return(tube);
   }

   private string _host;
   private ushort _port;
}

//------------------------------------------------------------------------------
// Unit Tests
//------------------------------------------------------------------------------
unittest {
   auto server = new Server("localhost");

   assert(server.host is "localhost");
   assert(server.port == Server.DEFAULT_BEANSTALKD_PORT);

   server = new Server("blah.com", 13214);

   assert(server.host is "blah.com");
   assert(server.port == 13214);
   assert(server.getTube() !is null);
}