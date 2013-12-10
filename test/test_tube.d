import core.thread, std.exception, std.stdio, test_case, stalkd;

class TestTube : TestCase {
   override void setup(ServerInfo serverInfo) {
      super.setup(serverInfo);
      _server = new Server(serverInfo.host, serverInfo.port);
      cleanup();
   }

   override void teardown() {
      cleanup();
   }

   void cleanup() {
      auto tubeNames = ["default", "test001"];
      auto tube      = _server.getTube();    

      foreach(name; tubeNames) {
         tube.using = name;
         tube.watch(name);
         while(tube.peek() !is null) {
            auto job = tube.reserve(2);

            if(job !is null) {
               job.destroy();
            }
         }
      }
   }

   override void run() {
      runBaseTests();
      cleanup();
      runDestroyTests();
      cleanup();
      runUsingTests();
      cleanup();
      runDelayedJobTests();
      cleanup();
      runPeekQueueTests();
      cleanup();
      runPeekIdTests();
      cleanup();
      runReleaseJobTests();
      cleanup();
      runBuryAndKickTests();
   }

   void runBaseTests() {
      auto tube = _server.getTube();

      assert(tube.connection !is null);
      assert(tube.using == "default");
      assert(tube.watching == ["default"]);
      assert(tube.peek() is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);
      assert(tube.peekForId(1000000) is null);
   }

   void runDestroyTests() {
      auto tube = _server.getTube();
      auto job  = new Job();

      job.append("This is a test job.");
      tube.put(job);
      assert(tube.peek() !is null);
      tube.reserve(2).destroy();
      assert(tube.peek is null);

      tube.using = "test001";
      tube.watch("test001");
      tube.ignore("default");

      tube.put(job);
      assert(tube.peek() !is null);
      tube.reserve(2).destroy();
      assert(tube.peek() is null);
   }

   void runUsingTests() {
      auto toTube   = _server.getTube("test001"),
           fromTube = _server.getTube();
      Job  inJob      = new Job,
           outJob;

      inJob.append("This is a test job.");

      assert(toTube.using == "test001");
      toTube.put(inJob);
      assertNotThrown!StalkdException(inJob.id);

      fromTube.using = "test001";
      assert(fromTube.peek() !is null);

      fromTube.watch("test001");
      outJob = fromTube.reserve(3);
      assert(outJob !is null);
      assert(outJob.id == inJob.id);
      assert(outJob.bodyAsString() == inJob.bodyAsString());
   }

   void runDelayedJobTests() {
      auto tube = _server.getTube();
      auto job  = new Job();

      assert(tube.peek() is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);

      job.append("This job was delayed.");
      tube.put(job, 3);

      assert(tube.peek() is null);
      assert(tube.peekDelayed() !is null);
      assert(tube.peekBuried() is null);

      Thread.getThis().sleep(dur!("seconds")(3));

      assert(tube.peek() !is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);

      job = tube.reserve();
      assert(job !is null);
      assert(job.bodyAsString() == "This job was delayed.");
   }

   void runPeekQueueTests() {
      auto tube = _server.getTube();
      auto job  = new Job();

      assert(tube.peek() is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);

      job.append("This is not a delayed job.");
      tube.put(job);
      assert(tube.peek() !is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);

      job = new Job;
      job.append("This is a delayed job.");
      tube.put(job, 3);
      assert(tube.peek() !is null);
      assert(tube.peekDelayed() !is null);
      assert(tube.peekBuried() is null);

      job = tube.reserve();
      assert(job !is null);
      assert(job.bodyAsString() == "This is not a delayed job.");
      Thread.getThis().sleep(dur!("seconds")(3));
      job = tube.reserve();
      assert(job !is null);
      assert(job.bodyAsString() == "This is a delayed job.");
   }

   void runPeekIdTests() {
      auto tube = _server.getTube();
      auto job  = new Job();
      uint jobId;

      assert(tube.peekForId(1000) is null);

      job.append("This is a test job.");
      tube.put(job);
      jobId = job.id;

      job = tube.peekForId(jobId);
      assert(job !is null);
      assert(job.id == jobId);
   }

   void runReleaseJobTests() {
      auto tube = _server.getTube();
      auto job  = new Job();

      job.append("Job used to test release.");
      tube.put(job);

      job = tube.reserve();
      assert(job !is null);
      assert(tube.peek() is null);

      tube.releaseJob(job.id);
      assert(tube.peek() !is null);

      job = tube.reserve();
      assert(job !is null);
      job.release(3);

      assert(tube.peek() is null);
      assert(tube.peekDelayed() !is null);

      Thread.getThis().sleep(dur!("seconds")(3));

      assert(tube.peek() !is null);
      assert(tube.peekDelayed() is null);
   }

   void runBuryAndKickTests() {
      auto tube = _server.getTube();
      Job job;
      uint jobId;

      job = new Job("This is job 1.");
      tube.put(job);
      job = new Job("This is job 2.");
      tube.put(job);
      job = new Job("This is job 3.");
      tube.put(job);

      assert(tube.peek() !is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);

      tube.reserve(1).bury();
      tube.reserve(1).bury();
      job = tube.reserve(1);
      job.release(60);

      assert(tube.peek() is null);
      assert(tube.peekDelayed() !is null);
      assert(tube.peekBuried() !is null);

      assert(tube.kick(2) == 2);
      assert(tube.peek() !is null);
      assert(tube.peekDelayed() !is null);
      assert(tube.peekBuried() is null);

      assert(tube.kick());
      assert(tube.peek() !is null);
      assert(tube.peekDelayed() is null);
      assert(tube.peekBuried() is null);

      job   = tube.reserve();
      jobId = job.id;
      job.bury();
      assert(tube.peekBuried() !is null);
      assert(tube.peekBuried().id == jobId);

      tube.kickJob(jobId);
      assert(tube.peekBuried() is null);
   }

   Server _server;
}