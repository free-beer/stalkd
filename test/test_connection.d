import std.stdio, test_case, stalkd;

class TestConnection : TestCase {
   override void setup(ServerInfo serverInfo) {
      super.setup(serverInfo);
      _server = new Server(serverInfo.host, serverInfo.port);
   }

   override void run() {
      auto connection = new Connection(_server);

      assert(!connection.isOpen);
      assert(connection.server is _server);

      connection.open();
      assert(connection.isOpen);

      connection.close();
      assert(!connection.isOpen);
   }

   Server _server;
}