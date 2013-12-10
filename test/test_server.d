import std.stdio, test_case, stalkd;

class TestServer : TestCase {
   override void run() {
      auto server = new Server("localhost");
      assert(server.host == "localhost");
      assert(server.port == Server.DEFAULT_BEANSTALKD_PORT);

      server = new Server("192.168.0.1", 4567);
      assert(server.host == "192.168.0.1");
      assert(server.port == 4567);
   }
}