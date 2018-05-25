// Copyright (c) 2013, Peter Wood.
// See license.txt for licensing details.
module stalkd.job;

import std.conv;
import std.outbuffer;
import stalkd.connection;
import stalkd.exceptions;
import stalkd.tube;

/**
 * This class models a Beanstalkd job. At it's lowest level a Job is really just
 * a collection of bytes but this function provides some addition functionality
 * for interacting with jobs.
 */
class Job {
   /**
    * The default priority given to jobs.
    */
   static const DEFAULT_PRIORITY = 0;

   /**
    * The default delay applied to jobs.
    */
   static const DEFAULT_DELAY = 0;

   /**
    * The default time to run applied to jobs.
    */
   static const DEFAULT_TIME_TO_RUN = 180;

   /**
    * Default constructor for the Job class.
    */
   this() {
      _buffer = new OutBuffer;
   }

   /**
    * Constructor for the Job class that allows it to handle strings of any
    * type.
    *
    * Params:
    *    data =  A string (or wstring or dstring) containing the job data.
    */
   this(T)(T[] data) {
      _buffer = new OutBuffer;
      append(data);
   }

   /**
    * Getter for the job id property. Note that this property will only be valid
    * for jobs that have been added to Beanstalk or extract from Beanstalk. If
    * called on a Job that doesn't meet these criteria an exception will be
    * thrown.
    */
   const @property uint id() {
      if(_tube is null) {
         throw(new StalkdException("Job doesn't yet possess a Beanstald id."));
      }

      return(_id);
   }

   /**
    * Setter for the job id property (visible only in package).
    */
   @property package void id(uint id) {
      _id = id;
   }

   /**
    * This function returns the Tube that a job was either put on or reserved
    * from. If the Job hasn't been put, reserved or peeked then this will be
    * null.
    */
   @property Tube tube() {
      return(_tube);
   }

   /**
    * Tube property setter, only accessible within the package.
    */
   @property package void tube(Tube tube) {
      _tube = tube;
   }

   /**
    * This function provides package level access to the data associated with
    * a Job object.
    *
    * Returns:  An array of ubytes containing the Job data.
    */
   @property ubyte[] data() {
      return(_buffer.toBytes());
   }

   /**
    * This function appends an array of ubytes to the data stored within a Job
    * object.
    *
    * Params:
    *    data =  The array of data to be written to the Job.
    */
   package void write(ubyte[] data) {
      if(data.length > 0) {
         _buffer.reserve(data.length);
         _buffer.write(data);
      }
   }

   /**
    * This function appends a string (or wstring or dstring) to the data stored
    * within a Job.
    *
    * Params
    *    data =  The string to be written into the Job.
    */
   void append(T)(T[] data) {
      write(cast(ubyte[])data);
   }

   /**
    * Fetches the body of the job, converting it to a string in the process.
    */
   string bodyAsString() {
      return(to!string(cast(char[])this.data));
   }

   /**
    * Fetches the body of the job, converting it to a dstring in the process.
    */
   dstring bodyAsDString() {
      return(to!dstring(cast(dchar[])this.data));
   }

   /**
    * Fetches the body of the job, converting it to a wstring in the process.
    */
   wstring bodyAsWString() {
      return(to!wstring(cast(wchar[])this.data));
   }

   /**
    * This function deletes a job from Beanstalk. Note that this function can
    * only be called on Jobs that have a been put into or reserved out of a
    * Beanstalk server.
    */
   void destroy() {
      if(_tube is null) {
         throw(new StalkdException("Job is not associated with a tube and cannot be deleted."));
      }
      _tube.deleteJob(_id);
   }

   /**
    * This function releases a job back to Beanstalk. Note that only reserved
    * jobs can be released.
    *
    * Params:
    *    delay =     The delay to be applied to the job as part of it's release.
    *                This defaults to DEFAULT_DELAY.
    *    priority =  The priority to be given to the job as it is released. This
    *                defaults to DEFAULT_PRIORITY.
    */
   void release(uint delay=DEFAULT_DELAY, uint priority=DEFAULT_PRIORITY) {
      if(_tube is null) {
         throw(new StalkdException("Job is not associated with a tube and cannot be released."));
      }
      _tube.releaseJob(_id, delay, priority);
   }

   /**
    * This function buries a job on Beanstalk. Note that only reserved jobs can
    * be buried.
    *
    * Params:
    *    priority =  The priority to be assigned to the buried job. Defaults to
    *                DEFAULT_PRIORITY.
    */
   void bury(uint priority= DEFAULT_PRIORITY) {
      if(_tube is null) {
         throw(new StalkdException("Job is not associated with a tube and cannot be buried."));
      }
      _tube.buryJob(_id, priority);
   }

   /**
    * This function touchs a job on Beanstalk, extending its time to run. Note
    * that only reserved jobs can be touched.
    */
   void touch() {
      if(_tube is null) {
         throw(new StalkdException("Job is not associated with a tube and cannot be touched."));
      }
      _tube.touchJob(_id);
   }

   private uint      _id;
   private OutBuffer _buffer;
   private Tube      _tube;
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
   import std.exception;
   import std.process;
   import std.stdio;
   import std.string;
   import stalkd;

   auto job = new Job("Test data.");

   void getId() {
      job.id;
   }
   void callBury() {
      job.bury();
   }
   void callDestroy() {
      job.destroy();
   }
   void callRelease() {
      job.release();
   }
   void callTouch() {
      job.touch();
   }

   // Test: Basic stuff.
   assertThrown!StalkdException(getId);
   assert(job.tube is null);
   assert(job.data == "Test data.".representation);
   assert(job.bodyAsString() == "Test data.");

   job.append(" Extra content.");
   assert(job.data == "Test data. Extra content.".representation);
   assert(job.bodyAsString() == "Test data. Extra content.");

   // Test: Operations when a tube hasn't been set.
   assertThrown!StalkdException(callBury);
   assertThrown!StalkdException(callDestroy);
   assertThrown!StalkdException(callRelease);
   assertThrown!StalkdException(callTouch);

   auto host = environment.get("BEANSTALKD_TEST_HOST");
   if(host !is null) {
      writeln("The BEANSTALKD_TEST_HOST environment variable is set, conducting advanced tests for the Job class.");
      ushort port = Server.DEFAULT_BEANSTALKD_PORT;
      if(environment.get("BEANSTALKD_TEST_PORT") !is null) {
         port = to!ushort(environment.get("BEANSTALKD_TEST_PORT"));
      }
      auto connection = new Connection(host, port);
      auto tube       = new Tube(connection);

      // Clear any existing content from the tube before starting.
      while((job = tube.peek()) !is null) {
         tube.deleteJob(job.id);
      }
      while((job = tube.peekBuried()) !is null) {
         tube.deleteJob(job.id);
      }

      // Test: Job id is accessible.
      job = new Job("Example test job.");
      tube.put(job);
      assertNotThrown!StalkdException(getId);

      // Test: Releasing a job.
      job = tube.reserve(3);
      assert(job !is null);
      assertNotThrown!StalkdException(callRelease);
      assert(tube.peek() !is null);

      // Test: Burying a job.
      job = tube.reserve(3);
      assert(job !is null);
      assertNotThrown!StalkdException(callBury);
      assert(tube.peek() is null);
      assert(tube.peekBuried() !is null);

      // Test: Destroying a job.
      assert(tube.kick() == 1);
      assert(tube.peek() !is null);
      job = tube.reserve();
      assert(job !is null);
      assertNotThrown!StalkdException(callDestroy);
      assert(tube.peek() is null);
   } else {
      writeln("The BEANSTALKD_TEST_HOST environment variable is not set, advanced tests for the Job class skipped.");
   }
}