// Copyright (c) 2013, Peter Wood.
// See license.txt for licensing details.

import std.algorithm, std.array, std.conv, std.outbuffer, std.socket, std.string;

/**
 * This class provides the exception class used by the library.
 */
class StalkdException : Exception {
   this(string message, Throwable thrown=null) {
      super(message, thrown);
   }
}

/**
 * This class encapsulates the details for a server running the Beanstalkd
 * application and provides a main interaction point for users to obtain
 * Tube objects.
 */
class Server {
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

/**
 * The Connection class represents a connection to a single Beanstalkd server.
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

/**
 * This class models a tube within a Beanstalkd instance. There are two concepts
 * associated with Tubes - watching and using. When you use a tube you alter the
 * target tube that new jobs get added to. When you watch a tube you are
 * indicating that you are interested in the jobs that have been added to it.
 * You can only use a single tube at any one time but you can watch multiple
 * tubes simultaneously.
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
         response.chomp();
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
               response.chomp();
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
               response.chomp();
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
    * This function attempts to reserve a job from one of the tubes that a Tube
    * object is currently watching.
    *
    * Params:
    *    timeOut = The maximum number of seconds for the server to wait for a
    *              job to become available. If no job is available then the
    *              function will return null. If set to zero, which is the
    *              default value, the function will block indefinitely.
    */
   Job reserve(uint timeOut=0) {
      Job    job;
      char[] response = new char[100];

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

         job      = new Job;
         job.id   = jobId;
         job.tube = this;
         job.write(buffer.toBytes());
      } else if(!response.startsWith("TIMED_OUT")) {
         response.chomp();
         throw(new StalkdException(to!string("2. Server returned a " ~ response ~ " error.")));
      }

      return(job);
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
    * This function releases a previosuly reserved job back to Beanstalk control. 
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

      _connection.socket.send(buffer.toBytes());
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

/**
 * This class models a Beanstalkd job. At it's lowest level a Job is really just
 * a collection of bytes but this function provides some addition functionality
 * for itnteracting with jobs.
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