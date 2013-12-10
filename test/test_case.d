import std.stdio;

struct ServerInfo {
   string host;
   ushort port;
}

class TestCase {
   void setup(ServerInfo serverInfo) {
      _serverInfo = serverInfo;
   }

   void teardown() {
   }

   bool execute(ServerInfo serverInfo) {
      bool pass = true;
      try {
         setup(serverInfo);
         run();
      } catch(Throwable exception) {
         stderr.writeln("Exception caught running test.\nERROR: ", exception.msg,
                        "\n", exception.file, " line ", exception.line);
         pass = false;
      }
      teardown();
      return(pass);
   }

   void run() {
   }

   ServerInfo _serverInfo;
}