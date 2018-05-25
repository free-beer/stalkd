// Copyright (c) 2013, Peter Wood.
// See license.txt for licensing details.
module stalkd.connection;

import std.socket;
import stalkd.exceptions;
import stalkd.server;
import stalkd.tube;

/**
 * The Connection class represents a connection to a single Beanstalkd server.
 * A Connection instance should not be shared between multiple tubes.
 * 
 */
class Connection {
   /**
    * Constructor for the Connection class.
    *
    * Params:
    *    server =  A reference to the Server the connection will attach to.
    */
   this(Server server) {
      _server = server;
   }

   /**
    * Constructor for the Connection class.
    */
   this(string host, ushort port=Server.DEFAULT_BEANSTALKD_PORT) {
      _server = Server(host, port);
   }

   /**
    * This function attempts to establish a connection to the server identified
    * by a Connection objects settings.
    */
   void open() {
      auto addresses = getAddress(_server.host, _server.port);

      if(addresses.length == 0) {
         throw(new StalkdException("Unable to determine a network address for the server."));
      }

      try {
         _socket = new TcpSocket;
         _socket.connect(addresses[0]);
      } catch(Exception exception) {
         throw(new StalkdException("Failed to open connection to server.", exception));
      }
   }

   /**
    * This function closes a connection if it is open.
    */
   void close() {
      if(_socket !is null) {
         _socket.close();
         _socket = null;
      }
   }

   /**
    * This function is used to test whether a connection is open.
    */
   const @property bool isOpen() {
      return(_socket !is null && _socket.isAlive);
   }

   /**
    * Getter for the server property.
    */
   @property Server server() {
      return(_server);
   }

   /**
    * This function provides package level access to the Socket held within a
    * Connection object. Asking for a socket before a Connection has been
    * explicitly opened implies an implicit call to the open() function.
    */
   @property package Socket socket() {
      if(_socket is null) {
         this.open();
      }
      return(_socket);
   }

   /**
    * This function retrieves a Tube object from a Connection.
    *
    * Params:
    *    name =  The name of the tube that the Tube object will use. Defaults
    *            to Tube.DEFAULT_TUBE_NAME.
    */
   Tube getTube(string name=Tube.DEFAULT_TUBE_NAME) {
      Tube tube = new Tube(this);

      if(name !is Tube.DEFAULT_TUBE_NAME) {
         tube.use(name);
      }

      return(tube);
   }

   private Server _server;
   private Socket _socket;
}

//------------------------------------------------------------------------------
// Unit Tests
//------------------------------------------------------------------------------
/*
 * NOTE: There is a limit to the amount of unit testing that can be performed
 *       without an actual server connection. For this reason, the test below
 *       check for the presence of an available test Beanstalkd instance via
 *       the existence of the BEANSTALKD_TEST_HOST environment variable. If
 *       this is set then an attempt will be made to connect to it to perform
 *       an additional series of tests. You can specify the port for this test
 *       server using the BEANSTALKD_TEST_PORT environment variable. As the
 *       queues on this server will be added to, deleted from and cleared of
 *       content as part of the tests this server should not be used for any
 *       other purpose!
 */
unittest {
   import std.stdio;
   import std.conv;
   import std.process;
   import std.exception;

   auto server     = Server("localhost");
   auto connection = new Connection(server);

   assert(connection.server is server);
   assert(connection.isOpen == false);

   auto tube = connection.getTube();
   assert(tube !is null);
   assert(tube.connection is connection);

   auto host = environment.get("BEANSTALKD_TEST_HOST");
   if(host !is null) {
      writeln("The BEANSTALKD_TEST_HOST environment variable is set, conducting advanced tests for the Connection class.");
      ushort port = Server.DEFAULT_BEANSTALKD_PORT;
      if(environment.get("BEANSTALKD_TEST_PORT") !is null) {
         port = to!ushort(environment.get("BEANSTALKD_TEST_PORT"));
      }

      connection = new Connection(host, port);

      void testOpen() {
         connection.open();
      }
      assertNotThrown!StalkdException(testOpen);
      assert(connection.isOpen == true);

      void testClose() {
         connection.close();
      }
      assertNotThrown!StalkdException(testClose);
      assert(connection.isOpen == false);
   } else {
      writeln("The BEANSTALKD_TEST_HOST environment variable is not set, advanced tests for the Connection class skipped.");
   }
}


