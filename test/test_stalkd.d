import std.conv, std.datetime, std.stdio, stalkd;
import test_case;
import test_server;
import test_tube;

void main(string[] arguments) {
   string[]   classNames = ["test_server.TestServer",
                            "test_connection.TestConnection",
                            "test_tube.TestTube"];
   ServerInfo serverInfo;

   serverInfo.host = "localhost";
   serverInfo.port = 11300;
   if(arguments.length > 0) {
      processArguments(arguments[1..$], serverInfo);
   }

   foreach(name; classNames) {
      try {
         TestCase test = cast(TestCase)Object.factory(name);

         writeln("Class: ", name);
         if(test.execute(serverInfo)) {
            writeln("Result: PASS\n-----");
         } else {
            writeln("Result: FAIL\n-----");
         }
      } catch(Throwable thrown) {
         stderr.writeln("Exception caught processing the ", name, " class.\nERROR: ",
                        thrown.msg, "\nLine ", thrown.line, " of ", thrown.file);
      }
   }
}

void processArguments(string[] arguments, ref ServerInfo serverInfo) {
   for(uint i; i < arguments.length; i++) {
      switch(arguments[i]) {
         case "-h":
            if(i + 1 < arguments.length) {
               serverInfo.host = arguments[++i];
            } else {
               stderr.writeln("WARNING: Incomplete -h argument specified.");
            }
            break;

         case "-p":
            if(i + 1 < arguments.length) {
               serverInfo.port = to!ushort(arguments[++i]);
            } else {
               stderr.writeln("WARNING: Incomplete -p argument specified.");
            }
            break;

         default:
            stderr.writeln("WARNING: Ignoring unrecognised command line parameter '", arguments[i], "'.");
      }
   }
}