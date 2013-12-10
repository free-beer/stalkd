# Stalkd

This library provides an interface to the Beanstalk message queue. The sections
below outline details of it's usage.

## License

The stalkd library is licensed under the terms of the MIT license. Details of
this license can be found in the license.txt file in the root of the project
source directory.

## Building The Library

The stalkd library use the dub package manager application. If you clone the
source repository and install dub you can build a production version of the
library using a command such as the following issued in the root directory of
the repository...

```
   $> dub build --build=release
```

The output from this command should be written into the bin subdirectory of
the repository and will consist of two files. On Linux systems these will be
called libstalkd.a and stalkd.di. The first is a static library the you can
compile into your application. The second is a header file that can be used
as an alternative to providing the source file for direct compilation (it's
needed by D to determine imports). Alternatively you can build a debugging
version of the library with the command...

```
   $> dub build --build=debug
```

## Using The Library

All of the components provided within the stalkd library are contained in
the stalkd module so you first have to import this to make use of any of the
libraries facilities. You can do this be adding a line such as the following
to your code...

```
   import stalkd;
```

Once you've imported the library you next need to create a Server object. This
requires the details for a host running the beanstalkd process. Examples of
how you might create a Server object would include...

```
   auto server1 = new Server("localhost"),
        server2 = new Server("192.168.0.1", 5678);
```

In the first example you create a Server that expects the beanstalkd process to
be running on localhost and on the default port of 11300. The second example
indicates that the server is at the specified IP address and listening on port
5678.

Once you have the server you can use it to obtain a Tube object which is the
main class for interacting with Beanstalk jobs. In Beanstalk there are two
concepts associated with tubes. Tubes can be used and they can be watched. A
used tube is one to which submitted jobs will be added. You can only be using
a single tube per Beanstalk connection. On the other hand you can be watching
multiple tubes simultaneously. Watched tubes are ones that you're interested
in knowing when jobs are available on them. Note that if a named tube does not
exist on the server when you specify it then it is auto-created by the server
itself.

Example code for obtaining a tube from a Server would be...

```
   auto tube1 = server.getTube(),
        tube2 = server.getTube("other");
```

The first tube we retrieve and store in tube1 is connected to the default tube
in Beanstalk (its name is actually "default"). Fetching a tube in this way we
are using and watching only the default tube. With the second tube we fetch we
specify the "other" name. This returns us a tube that is using the tube called
other and watching the "default" tube.

In the case of both tubes obtained above the type returned is stlakd.Tube. You
can change the tube you're using in one of two ways...

```
   tube1.use("blah");
   tube2.using = "ningy";
```

On the first line we just call the use function of the Tube object and specify
the name of the tube we want to start using. The second line just shows an
alternative approach by setting the using property but these two are effectively
the same behind the scenes.

Similarly there is a function for altering the tubes that a Tube object is
currently watching...

```
   tube1.watch("first", "second", "third");
```

This call adds three tubes to the list of tubes being watched by the Tube object
referred to as tube1. You can pass one or more tube names to a call to the
watch() function. Note, that calling watch() implies addition and not the
replacement of tubes being watched. To stop watching a tube then there use a
call like the following...

```
   tube1.ignore("default");
```

Again this function will accept one or more tube names. Once you have configured
your Tube object to watch the appropriate tubes you can fetch a job from it by
calling the reserve() function...

```
   Job job = tube1.reserve();
```

Note that in the example above the call to reserve will block until such time as
a job becomes available. If you want to use a non-blocking request then pass a
uint to the call to reserve() that specifies the maximum number of seconds that
the server will wait for a job to become available before giving up. In the
case of a job not being available a call to reserve() returns null.

The jobs returned from a call to reserve() are of type stalkd.Job. Beanstalk
considers all jobs to essentially be a collection of bytes. The Job class
provides some convenience methods for converting these collection of bytes
to and from strings. For example...

```
   string body = job.bodyAsString();
```

Note that use of these functions is contingent on the fact that the job was
originally written in the same encoding as you're trying to extract it into.
Reserving a job informs Beanstalk that you are interested in having sole
ownership of it and Beanstalk guarantees that the same job will not be handed
out to separate reservation requests. Reserving a job does not take it out of
the queue, to do that you must destroy it...

```
   job.destroy();
```

Destroying a job deletes it from Beanstalk. You should do this only when you
are satisfied that you have finished with the job. Note that when a job is
created in Beanstalk it has a time to run (TTR) value associated with it. This
is used by Beanstalk as a timer on the job. Beanstalk assumes that if you
reserve a job and then fail to destroy it within its TTR then it is free to
return it to the ready queue. If you do require extra time to process a job
you can extend the TTR by calling the touch() function of the Job class like
this...

```
   job.touch();
```

This resets the TTR timer for the job on the Beanstalk server. If while
processing the job you decide that you cannot continue working with the
job you've reserved you can return it to Beanstalks control by calling
the release() function...

```
   job.release();
```

The release() function accepts some additional parameters that are not shown
in this example, consult the code for details. Alternatively, if you decide
that the job cannot be processed but don't want to lose it you can bury it
instead. To bury a job make a call such as...

```
   job.bury();
```

Again the bury() function has a defaulted parameter so consult the code for
additional information. Finally, in relation to looking for jobs, if you simply
want to check that a job is available from the queue the you are currently using
you can call the peek() function on the tube, such as...

```
   Job job = tube1.peek();
```

This will return a job if there is one available or null if there isn't. Note
that you haven't reserved the job returned so you can't destroy it or bury it
as you haven't obtained exclusive access to it. This function is simply a
means of checking if any jobs are available. Note that there are other peek
functions on the Tube class, consult the code for more details.

Adding a job to Beanstalk involves creating a new Job object, populating it with
data and then submitting it to the server. This might looks like...

```
   auto job = new Job;

   job.append("This is the textual content of my job's body.");
   tube1.put(job);
```

This submits your job with a default priority and time to run and with no delay
(i.e. it's ready to be processed immediately). Here are some examples of adding
jobs that vary these parameters...

```
   // Add a job with a five minute delay.
   tube1.add(job, 300);

   // Add a job with no delay and a lower priority.
   tube1.add(job, 0, 1000);

   // Add a job with a 1 minutes delay, highest priority and a 10 minute TTR.
   tube1.add(job, 60, 0, 600);
```

## Thread Safety

There are no access control mechanisms on any of the classes or entities within
the library. Having said that the Server class is essentially immutable once
created and each Tube fetched from a Server gets it's own connection to the
Beanstalk server so you could share a Server instance between threads. You
certainly should not share Tubes between threads however.

## Testing

To build the unit test application for the library issue the following command
in the root directory of the repository...

```
   $> dub build --config=test
```

This should place a unit test executable into the bin directory upon completion.
Note, to run the test you must have a working instance of the Beanstalk server
that you can reference. By default the test application assumes it's running on
port 11300 or localhost. If this is not the case then you can specify -h and
-p flags when calling the executable to specify the host and port for the test
Beanstalk server.

## Issues

This is my first library in D v2 so it's probably more than a little rough
around the edges, comments and suggestions are welcome (or pull requests for
that matter). The unit tests are cobbled together as well as the languages
unit testing facilities aren't as advanced as scripting languages such as Ruby
and the classes within the library don't really lend themselves to techniques
such as mocking anyway.
