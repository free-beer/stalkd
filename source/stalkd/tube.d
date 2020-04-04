// Copyright (c) 2013, Peter Wood.
// See license.txt for licensing details.
module stalkd.tube;

import std.algorithm;
import std.conv;
import std.outbuffer;
import std.socket;
import std.string;
import std.typecons : Nullable;
import stalkd.connection;
import stalkd.exceptions;
import stalkd.job;
import stalkd.server;

/**
 * This class models a tube within a Beanstalkd instance. There are two concepts
 * associated with Tubes - watching and using. When you use a tube you alter the
 * target tube that new jobs get added to. When you watch a tube you are
 * indicating that you are interested in the jobs that have been added to it.
 * You can only use a single tube at any one time but you can watch multiple
 * tubes simultaneously.
 *
 * Note that the Connection object associated with a Tube instance should not
 * be shared with any other tubes. For this reason it's probably best practice
 * to create Tube's directly using the constructor that takes a Server object
 * as that will guarantee a new Connection for the Tube.
 */
class Tube {
   /**
    * The name of the default tube if an explicit tube is not used.
    */
   static const DEFAULT_TUBE_NAME = "default";

   /**
    * The maximum length permitted for a tube name.
    */
   static const MAX_TUBE_NAME_LEN = 200;

   /**
    * Constructor for the Tube class that creates a Tube object using the
    * 'default' tube on the server.
    *
    * Params:
    *    connection =  The Connection object for the server.
    */
   this(Connection connection) {
      _connection      = connection;
      _using           = DEFAULT_TUBE_NAME;
      _watching.length = 1;
      _watching[0]     = DEFAULT_TUBE_NAME;
   }

   /**
    * Constructor for the Tube class that creates a Tube object using the
    * 'default' tube on the server. Use this method in preference to creating
    * a Tube using a Connection object as this guarantees a Connection dedicated
    * to the Tube.
    *
    * Params:
    *    server =  The Beanstalk server to create the Tube for.
    */
   this(Server server) {
      this(new Connection(server));
   }

   /**
    * Getter for the connection property.
    */
   @property Connection connection() {
      return(_connection);
   }

   /**
    * This function retrieves a string containing the name of the tube that
    * the Tube object is currently using.
    */
   @property string using() {
      return(_using);
   }

   /**
    * This function is simply an alias for a call to the use() function.
    */
   @property void using(string name) {
      use(name);
   }

   /**
    * This function returns a list of the name for the tubes that the Tube
    * object is currently watching.
    */
   @property string[] watching() {
      return(_watching.dup);
   }

   /**
    * This function alters the server tube that a Tube object uses.
    *
    * Params:
    *    name =  The name of the tube to be used. Note that this string has to
    *            conform to Beanstalkd tube naming rules.
    */
   void use(string name) {
      if(name is null) {
         throw(new StalkdException("Tube name not specified."));
      } else if(name.length > MAX_TUBE_NAME_LEN) {
         throw(new StalkdException("Tube name too long."));
      }
      send(null, "use", name);

      auto response = receive();
      if(!response.startsWith("USING")) {
         response = response.chomp();
         throw(new StalkdException(to!string("Server responded with a " ~ response ~ " error.")));
      }
      _using = name;
   }

   /**
    * This function adds to the server tubes that a Tube object watches.
    *
    * Params:
    *    names =  An array of strings containing the names of the tubes to be
    *             watched. Invalid tube names will be completely ignored.
    */
   void watch(string[] names...) {
      foreach(name; names) {
         if(find(_watching, name).empty) {
            send(null, "watch", name);
            auto response = receive();
            if(!response.startsWith("WATCHING")) {
               response = response.chomp();
               throw(new StalkdException(to!string("Server responded with a " ~ response ~ " error.")));
            }
            _watching ~= name;
         }
      }
   }

   /**
    * This function removes one or more names from the server tubes that a Tube
    * object watches.
    *
    * Params:
    *    names =  An array of strings containing the names of the tubes to be
    *             ignored. Invalid tube names will be completely ignored.
    */
   void ignore(string[] names...){
      foreach(name; names) {
         if(!find(_watching, name).empty) {
            send(null, "ignore", name);
            auto response = receive();
            if(!response.startsWith("WATCHING")) {
               response = response.chomp();
               throw(new StalkdException(to!string("Server responded with a " ~ response ~ " error.")));
            }

            string[] remaining;
            foreach(entry; _watching) {
               if(entry != name) {
                  remaining ~= entry;
               }
            }
            _watching = remaining;
         }
      }
   }

   /**
    * This function adds a new job to the tube that is currently being used.
    *
    * Params:
    *    job =        A reference to the job to be added. Upin successful
    *                 addition of the job the objects id will be updated to
    *                 reflect the id given to it by Beanstalk.
    *    delay =      The delay to be assigned to the new job. Defaults to
    *                 Job.DEFAULT_DELAY.
    *    priority =   The priority to be allocated to the new job. Defaults to
    *                 Job.DEFAULT_PRIORITY.
    *    timeToRun =  The time to run to be allocated to the new job. Defaults
    *                 to Job.DEFAULT_TIME_TO_RUN.
    */
   void put(ref Job job, uint delay=Job.DEFAULT_DELAY, uint priority=Job.DEFAULT_PRIORITY, uint timeToRun=Job.DEFAULT_TIME_TO_RUN) {
      uint jobId;
      auto data = job.data;
   
      send(data, "put", priority, delay, timeToRun, data.length);

      auto response = receive();
      auto offset   = std.string.indexOf(response, " ");
      if(offset != -1) {
         jobId    = to!uint(response[++offset..$]);
         response = response[0..offset].stripRight();
      }

      if(response != "INSERTED") {
         StalkdException exception;

         if(response == "BURIED") {
             throw(new StalkdException(to!string("Server had insufficient memory to grow the priority queue. Job id " ~ to!string(jobId) ~ " was buried.")));
         } else if(response == "JOB_TOO_BIG") {
            throw(new StalkdException("Job is too big."));
         } else if(response == "DRAINING") {
            throw(new StalkdException("Server is not accepting new jobs at this time."));
         } else if(response == "EXPECTED_CRLF") {
            throw(new StalkdException("Internal message structure error."));
         } else {
            throw(new StalkdException(to!string("1. Server returned a " ~ response ~ " error.")));
         }
      } else {
         job.id   = jobId;
         job.tube = this;
      }
   }

   /**
    * A blocking implementation of the reserve() method that will not return
    * until such time as a Job is available or an exception occurs.
    */
   Job reserve() {
      return(reserve(0).get());
   }

   /**
    * This function attempts to reserve a job from one of the tubes that a Tube
    * object is currently watching. Note that the return type for the function
    * is a Nullable!Job. This value will test as null if a Job did not become
    * available before the time out.
    *
    * Params:
    *    timeOut = The maximum number of seconds for the server to wait for a
    *              job to become available. If no job is available then the
    *              function will return null. If set to zero  the function will
    *              block indefinitely (i.e. it won't time out).
    */
   Nullable!Job reserve(uint timeOut) {
      Nullable!Job output;
      char[]       response = new char[100];

      if(timeOut > 0) {
         send(null, "reserve-with-timeout", timeOut);
      } else {
         send(null, "reserve");
      }

      auto total = _connection.socket.receive(response);
      if(total == Socket.ERROR) {
         throw(new StalkdException("Error reading from server connection."));
      } else if(total == 0) {
         throw(new StalkdException("Connection to server unexpectedly terminated."));
      }
      response = response[0..total];

      if(response.startsWith("RESERVED")) {
         uint      jobId;
         ulong     read,
                   size;
         size_t[]  offsets = [0, 0, 0];
         OutBuffer buffer;

         offsets[0] = std.string.indexOf(response, " ");
         offsets[1] = std.string.indexOf(response, " ", (offsets[0] + 1));
         offsets[2] = std.string.indexOf(response, "\r\n", (offsets[1] + 1));
         if(!offsets.find(-1).empty) {
            throw(new StalkdException("Unrecognised response received from server."));
         }

         jobId  = to!uint(response[(offsets[0] + 1)..offsets[1]]);
         size   = to!uint(response[(offsets[1] + 1)..offsets[2]]);
         read   = response.length - (offsets[2] + 2);
         buffer = new OutBuffer;
         buffer.reserve(cast(uint)size);

         if(read > 0) {
            auto endPoint  = response.length,
                 available = endPoint - (offsets[2] + 2);

            while(available > size) {
               endPoint--;
               available = endPoint - (offsets[2] + 2);
            }

            buffer.write(response[(offsets[2] + 2)..endPoint]);
         }
         if(size > read) {
            readInJobData(buffer, cast(uint)(size - read));
         }

         auto job = new Job;
         job.id   = jobId;
         job.tube = this;
         job.write(buffer.toBytes());
         output   = job;
      } else if(!response.startsWith("TIMED_OUT")) {
         response.chomp();
         throw(new StalkdException(to!string("2. Server returned a " ~ response ~ " error.")));
      }

      return(output);
   }

   /**
    * This function attempts to kick buried jobs. If there are buried jobs then
    * Beanstalk will return them to a ready state. Failing that, if there are
    * any delayed jobs they will be kicked instead.
    *
    * Params:
    *    maximum =  The maximum number of jobs to kick. Defaults to 1.
    */
   public uint kick(uint maximum=1) {
      uint total;

      send(null, "kick", maximum);
      auto response = receive();
      if(response.startsWith("KICKED")) {
         total = to!uint(response[(std.string.indexOf(response, " ") + 1)..$]);
      } else {
         throw(new StalkdException(to!string("Server responded with a " ~ response ~ " error.")));
      }
      return(total);
   }

   /**
    * This function kicks a specific job if it is sitting in the buried or
    * delayed queues.
    */
   public void kickJob(uint jobId) {
      send(null, "kick-job", jobId);
      auto response = receive();
      if(response != "KICKED") {
         throw(new StalkdException(to!string("Server responded with a " ~ response ~ " error.")));
      }
   }

   /**
    * This function 'peeks' at the Beanstalk ready queue to see if there is a
    * job available. If there is a job is is returned. Note that peeking does
    * not reserve the job returned.
    *
    * Returns:  A Job if one is available, null otherwise.
    */
   public Job peek() {
      return(peekFor("ready"));
   }

   /**
    * This function 'peeks' at the Beanstalk delayed queue to see if there is a
    * job available. If there is a job is is returned. Note that peeking does
    * not reserve the job returned.
    *
    * Returns:  A Job if one is available, null otherwise.
    */
   public Job peekDelayed() {
      return(peekFor("delayed"));
   }

   /**
    * This function 'peeks' at the Beanstalk buried queue to see if there is a
    * job available. If there is a job is is returned. Note that peeking does
    * not reserve the job returned.
    *
    * Returns:  A Job if one is available, null otherwise.
    */
   public Job peekBuried() {
      return(peekFor("buried"));
   }

   /**
    * This function peeks at Beanstalks contents to see if a job with a given
    * id exists. If it does it is returned. Note that peeking does not reserve
    * the job returned.
    *
    * Returns:  A Job if the job exists, null otherwise.
    *
    * Params:
    *    jobId =  The unique identifier of the job to peek for.
    */
   public Job peekForId(uint jobId) {
      return(doPeek(to!string("peek " ~ to!string(jobId))));
   }

   /**
    * This function deletes a specific job from Beanstalk. Note that you must
    * have reserved the job before you can delete it.
    *
    * Params:
    *    jobId =  The unique identifier of the job to delete.
    */
   public void deleteJob(uint jobId) {
      send(null, "delete", jobId);
      auto response = receive();
      if(response != "DELETED") {
         throw(new StalkdException(to!string("3. Server returned a " ~ response ~ " error.")));
      }
   }

   /**
    * This function releases a previously reserved job back to Beanstalk control. 
    *
    * Params:
    *    jobId =  The unique identifier of the job to released.
    *    delay =     The delay to be applied to the job when it is released
    *                back to Beanstalk. Defaults to Job.DEFAULT_DELAY.
    *    priority =  The priority to be applied to the job when it is released
    *                back to Beanstalk. Defaults to Job.DEFAULT_PRIORITY.
    */
   public void releaseJob(uint jobId, uint delay=Job.DEFAULT_DELAY, uint priority=Job.DEFAULT_PRIORITY) {
      send(null, "release", jobId, priority, delay);
      auto response = receive();
      if(response == "BURIED") {
         throw(new StalkdException(to!string("Server had insufficient memory to grow its priority queue. Job id " ~ to!string(jobId) ~ " was buried.")));
      } else if(response != "RELEASED") {
         throw(new StalkdException(to!string("4. Server returned a " ~ response ~ " error.")));
      }
   }

   /**
    * This function buries a specified job. Note that you must have first
    * reserved the job before you can bury it.
    *
    * Params:
    *    jobId =     The unique identifier of the job to bury.
    *    priority =  The priority to assign to the job as part of burying it.
    *                Defaults to Job.DEFAULT_PRIORITY.
    */
   public void buryJob(uint jobId, uint priority=Job.DEFAULT_PRIORITY) {
      send(null, "bury", jobId, priority);
      auto response = receive();
      if(response != "BURIED") {
         throw(new StalkdException(to!string("5. Server returned a " ~ response ~ " error.")));
      }
   }

   /**
    * This function touches a job, extending its time to run on the server. Note
    * that you must have first reserved the job before you can touch it.
    *
    * Params:
    *    jobId =  The unique identifier of the job to touch.
    */
   public void touchJob(uint jobId) {
      send(null, "touch", jobId);
      auto response = receive();
      if(response != "TOUCHED") {
         throw(new StalkdException(to!string("6. Server returned a " ~ response ~ " error.")));
      }
   }

   /**
    * This function is used internally by the class to dispatch requests to
    * the Beanstalk server.
    *
    * Params:
    *    data =       The data to be sent in the request. If null then no
    *                 data is sent.
    *    parameters = The parameters to be prefixed to the data being sent.
    */
   private void send(T...)(in ubyte[] data, T parameters) {
      OutBuffer buffer = new OutBuffer;
      string    request;
      uint      index;

      foreach(parameter; parameters) {
         if(index > 0) {
            request ~= " ";
         }
         request ~= to!string(parameter);
         index++;
      }
      request ~= "\r\n";
      buffer.reserve(request.length + (data ? data.length : 0) + 2);
      buffer.write(request);

      if(data !is null && data.length > 0) {
         buffer.write(data);
         buffer.write("\r\n");
      }

      if(_connection.socket.send(buffer.toBytes()) == Socket.ERROR) {
         throw(new StalkdException("Error sending data on server connection."));
      }
   }

   /**
    * This function is used internally by the class wherever a simple answer is
    * expected to a request.
    *
    * Returns:  A string containing the response value read. Note that trailing
    *           whitespace on the response will have been removed.
    */
   private string receive() {
      char[] response = new char[100];
      auto   total    = _connection.socket.receive(response);

      if(total == Socket.ERROR) {
         throw(new StalkdException("Error reading from server connection."));
      } else if(total == 0) {
         throw(new StalkdException("Connection to server unexpectedly terminated."));
      }

      return(to!string(response[0..total]).chomp());
   }

   /**
    * This function us used internally by the class to read job data into an
    * OutBuffer instance.
    *
    * Params:
    *    buffer =    The buffer to place the bytes read into.
    *    quantity =  The number of bytes of data to be read in.
    */
   private void readInJobData(ref OutBuffer buffer, uint quantity) {
      ubyte[] data  = new ubyte[quantity + 2];
      auto    total = _connection.socket.receive(data);

      if(total == Socket.ERROR) {
         throw(new StalkdException("Error retrieving response from server."));
      } else if(total == 0) {
         throw(new StalkdException("Server connection closed unexpectedly."));
      }
      data = data[0..($ - 2)];
      buffer.write(data);
   }

   /**
    * This function is used internally by the class to check for available jobs
    * of a specified type.
    *
    * Params:
    *    type =  A string that should be either "ready", "delayed" or "buried". 
    */
   private Job peekFor(string type) {
      return(doPeek(to!string("peek-" ~ type)));
   }

   /**
    * This function performs a peek operation against the server.
    *
    * Params:
    *    request =  The request to be sent to the server. 
    */
   private Job doPeek(string request) {
      Job    job      = null;
      char[] response = new char[100];

      send(null, request);

      auto total = _connection.socket.receive(response);
      if(total == Socket.ERROR) {
         throw(new StalkdException("Error reading from server connection."));
      } else if(total == 0) {
         throw(new StalkdException("Connection to server unexpectedly terminated."));
      }
      response = response[0..total].chomp();

      if(response.startsWith("FOUND")) {
         uint      jobId;
         ulong     size,
                   read;
         size_t[]  offsets = [0, 0, 0];
         OutBuffer buffer;

         offsets[0] = std.string.indexOf(response, " ");
         offsets[1] = std.string.indexOf(response, " ", (offsets[0] + 1));
         offsets[2] = std.string.indexOf(response, "\r\n", (offsets[1] + 1));
         if(!offsets.find(-1).empty) {
            throw(new StalkdException("Unrecognised response received from server."));
         }

         jobId     = to!uint(response[(offsets[0] + 1)..offsets[1]]);
         size      = to!size_t(response[(offsets[1] + 1)..offsets[2]]);
         read      = response.length - (offsets[2] + 2);
         buffer = new OutBuffer;
         buffer.reserve(cast(uint)size);

         if(read > 0) {
            auto endPoint  = response.length,
                 available = endPoint - (offsets[2] + 2);

            while(available > size) {
               endPoint--;
               available = endPoint - (offsets[2] + 2);
            }

            buffer.write(response[(offsets[2] + 2)..$]);
         }
         if(size > read) {
            readInJobData(buffer, cast(uint)(size - read));
         }

         job      = new Job;
         job.id   = jobId;
         job.tube = this;
         job.write(buffer.toBytes());
      } else if(!response.startsWith("NOT_FOUND")) {
         throw(new StalkdException(to!string("7. Server returned a " ~ response ~ " error.")));
      }

      return(job);
   }

   private Connection _connection;
   private string     _using;
   private string[]   _watching;
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
   import core.thread;
   import core.time;
   import std.stdio;
   import std.conv;
   import std.process;
   import std.exception;
   import stalkd;

   auto connection = new Connection("127.0.0.1");
   auto tube       = new Tube(connection);   

   assert(tube.connection is connection);
   assert(tube.using is Tube.DEFAULT_TUBE_NAME);
   assert(tube.watching == [Tube.DEFAULT_TUBE_NAME]);  

   auto host = environment.get("BEANSTALKD_TEST_HOST");
   if(host !is null) {
      writeln("The BEANSTALKD_TEST_HOST environment variable is set, conducting advanced tests for the Tube class.");
      ushort port = Server.DEFAULT_BEANSTALKD_PORT;
      if(environment.get("BEANSTALKD_TEST_PORT") !is null) {
         port = to!ushort(environment.get("BEANSTALKD_TEST_PORT"));
      }
      connection = new Connection(host, port);

      string tubeName = "alternative";
      tube = new Tube(connection);

      void useTube() {
         tube.use(tubeName);
      }

      void watchTube() {
         tube.watch(tubeName);
      }

      void ignoreTube() {
         tube.ignore(tubeName);
      }

      // Test: Use a tube name.
      assertNotThrown!StalkdException(useTube);
      assert(tube.using is tubeName);
      assert(tube.watching == [Tube.DEFAULT_TUBE_NAME]);

      // Test: Use can switch between tube names multiple times.
      tubeName = Tube.DEFAULT_TUBE_NAME;
      assertNotThrown!StalkdException(useTube);
      assert(tube.using is tubeName);
      assert(tube.watching == [Tube.DEFAULT_TUBE_NAME]);

      // Test: Watch a tube name.
      tubeName = "alternative";
      assertNotThrown!StalkdException(watchTube);
      assert(tube.using is Tube.DEFAULT_TUBE_NAME);
      assert(tube.watching == [Tube.DEFAULT_TUBE_NAME, tubeName]);

      // Test: Ignore a tube name.
      assertNotThrown!StalkdException(ignoreTube);
      assert(tube.using is Tube.DEFAULT_TUBE_NAME);
      assert(tube.watching == [Tube.DEFAULT_TUBE_NAME]);

      // Test: The default tube name can be ignored.
      assertNotThrown!StalkdException(watchTube);
      tubeName = Tube.DEFAULT_TUBE_NAME;
      assertNotThrown!StalkdException(ignoreTube);
      assert(tube.using is Tube.DEFAULT_TUBE_NAME);
      assert(tube.watching == ["alternative"]);

      // Test: You can't unwatch all tubes.
      tubeName = "alternative";
      assertThrown!StalkdException(ignoreTube);

      // Clear any existing content from the tube before starting.
      Job job;
      while((job = tube.peek()) !is null) {
         tube.deleteJob(job.id);
      }
      while((job = tube.peekBuried()) !is null) {
         tube.deleteJob(job.id);
      }

      // Put a job into a tube.
      job  = new Job("Job data.");
      tube = new Tube(Server(host, port));
      void putJob() {
         tube.put(job);
      }
      assertNotThrown!StalkdException(putJob);

      // Test: Peek to see if the job is there.
      void peekJob() {
         job = tube.peek();
      }
      assertNotThrown!StalkdException(peekJob);
      assert(job !is null);
      assert(job.bodyAsString() == "Job data.");

      // Test: Reserve a job without timeout.
      void reserveJob() {
         job = tube.reserve();
      }
      assertNotThrown!StalkdException(reserveJob);
      assert(job.bodyAsString() == "Job data.");

      // Test: Releasing a job.
      void releaseJob() {
         tube.releaseJob(job.id);
      }
      assertNotThrown!StalkdException(releaseJob);

      // Test: Reserve a job from a tube with timeout.
      Nullable!Job reserved;
      void reserveJobWithTimeOut() {
         reserved = tube.reserve(3);
         if(!reserved.isNull) {
            job = reserved.get();
         }
      }
      assertNotThrown!StalkdException(reserveJobWithTimeOut);
      assert(!reserved.isNull);
      assert(job.bodyAsString() == "Job data.");
      assertNotThrown!StalkdException(releaseJob);

      // Test: Deleting a job.
      void deleteJob() {
         tube.deleteJob(job.id);
      }
      assertNotThrown!StalkdException(reserveJob);
      assertNotThrown!StalkdException(deleteJob);
      assert(tube.peek() is null);

      // Test: Burying a job.
      void buryJob() {
         tube.buryJob(job.id);
      }
      void peekBuried() {
         job = tube.peekBuried();
      }
      job = new Job("A different set of job data.");
      assertNotThrown!StalkdException(putJob);
      assertNotThrown!StalkdException(reserveJob);
      assertNotThrown!StalkdException(buryJob);
      assertNotThrown!StalkdException(peekBuried);
      assert(job !is null);
      assert(job.bodyAsString() == "A different set of job data.");

      // Test: Kicking a job.
      auto kicked = 0;
      void kickJob() {
         kicked = tube.kick(100);
      }
      assertNotThrown!StalkdException(kickJob);
      assert(kicked == 1);
      assert(tube.peek() !is null);
      assertNotThrown!StalkdException(deleteJob);

      // Test: Touching a job.
      void touchJob() {
         tube.touchJob(job.id);
      }
      assertNotThrown!StalkdException(putJob);
      assertNotThrown!StalkdException(reserveJob);
      assertNotThrown!StalkdException(touchJob);
      assertNotThrown!StalkdException(releaseJob);
      assertNotThrown!StalkdException(peekJob);
      assert(job !is null);
      assert(job.bodyAsString() == "A different set of job data.");
      assertNotThrown!StalkdException(deleteJob);
   } else {
      writeln("The BEANSTALKD_TEST_HOST environment variable is not set, advanced tests for the Tube class skipped.");
   }
}