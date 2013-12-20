// D import file generated from 'src\stalkd.d'
import std.algorithm;
import std.array;
import std.conv;
import std.outbuffer;
import std.socket;
import std.string;
class StalkdException : Exception
{
	this(string message, Throwable thrown = null);
}
class Server
{
	static const DEFAULT_BEANSTALKD_PORT = 11300;
	this(string host, ushort port = DEFAULT_BEANSTALKD_PORT);
	@property string host();

	@property ushort port();

	Tube getTube(string name = Tube.DEFAULT_TUBE_NAME);
	private string _host;

	private ushort _port;

}
class Connection
{
	this(Server server);
	void open();
	void close();
	const @property bool isOpen();

	@property Server server();

	@property package Socket socket();


	Tube getTube(string name = Tube.DEFAULT_TUBE_NAME);
	private Server _server;

	private Socket _socket;

}
class Tube
{
	static const DEFAULT_TUBE_NAME = "default";
	static const MAX_TUBE_NAME_LEN = 200;
	this(Connection connection);
	@property Connection connection();

	@property string using();

	@property void using(string name);

	@property string[] watching();

	void use(string name);
	void watch(string[] names...);
	void ignore(string[] names...);
	void put(ref Job job, uint delay = Job.DEFAULT_DELAY, uint priority = Job.DEFAULT_PRIORITY, uint timeToRun = Job.DEFAULT_TIME_TO_RUN);
	Job reserve(uint timeOut = 0);
	public uint kick(uint maximum = 1);

	public void kickJob(uint jobId);

	public Job peek();

	public Job peekDelayed();

	public Job peekBuried();

	public Job peekForId(uint jobId);

	public void deleteJob(uint jobId);

	public void releaseJob(uint jobId, uint delay = Job.DEFAULT_DELAY, uint priority = Job.DEFAULT_PRIORITY);

	public void buryJob(uint jobId, uint priority = Job.DEFAULT_PRIORITY);

	public void touchJob(uint jobId);

	private template send(T...)
	{
		void send(in ubyte[] data, T parameters)
		{
			OutBuffer buffer = new OutBuffer;
			string request;
			uint index;
			foreach (parameter; parameters)
			{
				if (index > 0)
				{
					request ~= " ";
				}
				request ~= to!string(parameter);
				index++;
			}
			request ~= "\x0d\x0a";
			buffer.reserve(request.length + (data ? data.length : 0) + 2);
			buffer.write(request);
			if (data !is null && data.length > 0)
			{
				buffer.write(data);
				buffer.write("\x0d\x0a");
			}
			_connection.socket.send(buffer.toBytes());
		}

	}

	private string receive();

	private void readInJobData(ref OutBuffer buffer, uint quantity);

	private Job peekFor(string type);

	private Job doPeek(string request);

	private Connection _connection;

	private string _using;

	private string[] _watching;

}
class Job
{
	static const DEFAULT_PRIORITY = 0;
	static const DEFAULT_DELAY = 0;
	static const DEFAULT_TIME_TO_RUN = 180;
	this();
	template __ctor(T)
	{
		this(T[] data)
		{
			_buffer = new OutBuffer;
			append(data);
		}

	}
	const @property uint id();

	@property package void id(uint id);


	@property Tube tube();

	@property package void tube(Tube tube);


	@property ubyte[] data();

	package void write(ubyte[] data);

	template append(T)
	{
		void append(T[] data)
		{
			write(cast(ubyte[])data);
		}

	}
	string bodyAsString();
	dstring bodyAsDString();
	wstring bodyAsWString();
	void destroy();
	void release(uint delay = DEFAULT_DELAY, uint priority = DEFAULT_PRIORITY);
	void bury(uint priority = DEFAULT_PRIORITY);
	void touch();
	private uint _id;

	private OutBuffer _buffer;

	private Tube _tube;

}
