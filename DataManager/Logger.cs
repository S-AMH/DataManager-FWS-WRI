using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace Logger
{
    public sealed class Logger : IDisposable
    {
        [Flags]
        ///<summary>
        ///Indicates Logger's target.
        ///<list type="bullet">
        ///<item>
        ///<term>Console</term>
        ///<description>Logger will log all events in console.</description>
        ///</item>
        ///<term>File</term>
        ///<description>Logger will log all events in a text format with .log extention.</description>
        /// </list>
        ///</summary>
        public enum Targets
        {
            Console,
            File
        }
        /// <summary>
        /// Indicates Log messaegs different types and their meaning.
        /// <list type="bullet">
        /// <item>
        /// <term>Debug</term>
        /// <description>
        /// Messages with Debug purpose.
        /// </description>
        /// </item>
        /// <item>Info</item>
        /// <description>Messages which contain general information about tasks.</description>
        /// <item>Warn</item>
        /// <description>Messages contaning warnings about application.</description>
        /// <item>Error</item>
        /// <description>Errors occured in application</description>
        /// <item>Fatal</item>
        /// <description>Errors that cause application to crash or stop working. </description>
        /// </list>
        /// </summary>
        public enum Level
        {
            Debug,
            Info,
            Warn,
            Error,
            Fatal
        }

        public const Logger Null = null;
        /// <summary>
        /// Logger identifier. Each Logger has its own ID which is used to write in log files and displaying in console.
        /// </summary>
        private string id;
        /// <summary>
        /// each logger has a target platform. platforms currently included are Console and File.
        /// </summary>
        private Targets targets;
        /// <summary>
        /// Used to prevent diffrenet loggers to access same log file or effect other loggers and processes work.
        /// </summary>
        private object lockObject;
        /// <summary>
        /// Sotores incoming <paramref name="Entry"/> requests to be written.
        /// </summary>
        private Queue<Entry> entries;
        /// <summary>
        /// Thread which current logger is running in.
        /// </summary>
        private Thread thread;
        /// <summary>
        /// Indicates whether logger is running or not. True: Logger is working, False: Logger is stoped.
        /// </summary>
        private bool isRunning;

        /// <summary>
        /// Each log is made out of an Entry which is sent to Logger in order to be logged.
        /// </summary>
        private class Entry
        {
            /// <summary>
            /// date and time used to Time stamp the log.
            /// </summary>
            private DateTime dateTime;
            /// <summary>
            /// Indicates log type.
            /// <see cref="Logger.Level"/>
            /// </summary>
            private Level level;
            /// <summary>
            /// stores Loggers thread number.
            /// </summary>
            private int threadId;
            /// <summary>
            /// Log message.
            /// </summary>
            private string message;

            /// <summary>
            /// Default constructor for Entry Class. Creats an entry to be logged by Logger.
            /// </summary>
            /// <remarks> all CF or LF or \t in message string will be replaced by "|" character. </remarks>
            /// <param name="dateTime">Time stamp for log message <see cref="System.DateTime"/></param>
            /// <param name="level">log type <see cref="Level"/></param>
            /// <param name="threadId">Logger thread ID <see cref="thread"/></param>
            /// <param name="message">log message <see cref="message"/></param>
            public Entry(DateTime dateTime, Level level, int threadId, string message)
            {
                this.dateTime = dateTime;
                this.level = level;
                this.threadId = threadId;
                this.message = message.Replace("\t", "|").Replace("\r", "|").Replace("\n", "|");
            }

            /// <summary>
            /// Time stamp property for log.
            /// </summary>
            /// <value> This property gets value of log's timeStamp as <paramref name="System.DateTime"/></value>
            /// <see cref="Entry.dateTime"/> 
            public DateTime DateTime { get { return dateTime; } }
            /// <summary>
            /// Log type property for log.
            /// </summary>
            /// <value> This property gets log type</value>
            /// <see cref="Entry.level"/>
            public Level Level { get { return level; } }
            /// <summary>
            /// ThreadID Property for log
            /// </summary>
            /// <value>This property gets log's Associated ThreadID</value>
            public int ThreadId { get { return threadId; } }
            /// <summary>
            /// Message Property for log.
            /// </summary>
            /// <value>This property gets log's Message</value>
            public string Message { get { return message; } }
            /// <summary>
            /// Writes log to console window in following format -> yyyy.MM.dd HH:mm:ss.fff level threadID message
            /// </summary>
            /// <remarks>Each one is seperated by Tab</remarks>
            public void WriteToConsole()
            {
                var dateTime2 = dateTime.ToString("yyyy.MM.dd HH:mm:ss.fff");
                var line = string.Format("{0}\t{1}\t{2}\t{3}", dateTime2, level, threadId, message);
                Console.WriteLine(line);
            }
            /// <summary>
            /// Writes log to File in following format -> file name = yyyyMMdd.log OR yyyyMMdd_loggerId.log
            /// Log formats are in following format -> HH:mm:ss.ff level theadId message
            /// </summary>
            /// <remarks>time level threadId and message are seperated by Tab</remarks>
            /// <param name="loggerId"></param>
            public void WriteToFile(string loggerId)
            {
                var date = dateTime.ToString("yyyyMMdd");
                var logFileName = string.Format(string.IsNullOrEmpty(loggerId) ? "{0}.log" : "{0}_{1}.log", date, loggerId);
                using (var streamWriter = new StreamWriter(logFileName, true))
                {
                    var time = dateTime.ToString("HH:mm:ss.fff");
                    var line = string.Format("{0}\t{1}\t{2}\t{3}", time, level, threadId, message);
                    streamWriter.WriteLine(line);
                }
            }
        }
        /// <summary>
        /// Run logger. When logger is running, the thread will be locked.
        /// </summary>
        /// <remarks>There is 10 milliseconds wait time before writing next log.</remarks>
        /// <exception cref="System.Exception">Thrown When something goes wrong with wrting OR displaying log.</exception>
        private void Run()
        {
            while (true)
            {
                lock (lockObject)
                {
                    if (entries.Count == 0)
                    {
                        if (!isRunning)
                            break;
                    }
                    else
                    {
                        try
                        {
                            var entry = entries.Dequeue();
                            if (targets.HasFlag(Targets.Console))
                                entry.WriteToConsole();
                            if (targets.HasFlag(Targets.File))
                                entry.WriteToFile(id);
                        }
                        catch (Exception exception)
                        {
                            Console.WriteLine(exception);
                            isRunning = false;
                        }
                    }
                }
                Thread.Sleep(10);
            }
        }
        /// <summary>
        /// Default Constructor for Logger class. Creates empty Queue of entries, null thread and isRunning flag is set to flase.
        /// </summary>
        /// <param name="id">Logger's ID <see cref="id"/></param>
        /// <param name="targets">Specifies Target platform for logger. <see cref="Targets"/></param>
        public Logger(string id, Targets targets)
        {
            this.id = id;
            this.targets = targets;
            lockObject = new object();
            entries = new Queue<Entry>();
            thread = null;
            isRunning = false;
        }
        /// <summary>
        /// If logger is not working, it will start logger. assignes new thread to logger. 
        /// </summary>
        public void Start()
        {
            lock (lockObject)
            {
                if (isRunning)
                    return;
                isRunning = true;
            }
            thread = new Thread(Run);
            thread.Start();
        }
        /// <summary>
        /// If logger is working, it will stop logger. cleans assigned thread to logger.
        /// </summary>
        public void Stop()
        {
            lock (lockObject)
            {
                if (!isRunning)
                    return;
                isRunning = false;
            }
            thread.Join();
            thread = null;
        }
        /// <summary>
        /// Add new log to Logger's entry queue. Time stamp is equal to the time when log enters this function.
        /// </summary>
        /// <param name="level">log's piority level. <see cref="Level"/></param>
        /// <param name="message">log's message. <see cref="Entry.message"/></param>
        public void Log(Level level, string message)
        {
            lock (lockObject)
            {
                if (!isRunning)
                    return;
                var timeStamp = DateTime.Now;
                var threadId = Thread.CurrentThread.ManagedThreadId;
                var entry = new Entry(timeStamp, level, threadId, message);
                entries.Enqueue(entry);
            }
        }
        /// <summary>
        /// Submits a Debug log message.
        /// </summary>
        /// <param name="message">log message <see cref="Entry.message"/></param>
        public void Debug(string message) { Log(Level.Debug, message); }
        /// <summary>
        /// Submits an Info log message
        /// </summary>
        /// <param name="message">log message <see cref="Entry.message"/></param>
        public void Info(string message) { Log(Level.Info, message); }
        /// <summary>
        /// Submits a warnign log message
        /// </summary>
        /// <param name="message">log message <see cref="Entry.message"/></param>
        public void Warn(string message) { Log(Level.Warn, message); }
        /// <summary>
        /// Submits an Error log message.
        /// </summary>
        /// <param name="message">log message <see cref="Entry.message"/></param>
        public void Error(string message) { Log(Level.Error, message); }
        /// <summary>
        /// Submits a fatal log message.
        /// </summary>
        /// <param name="message">log message <see cref="Entry.message"/></param>
        public void Fatal(string message) { Log(Level.Fatal, message); }
        /// <summary>
        /// Creates a log with specified format.
        /// </summary>
        /// <param name="level">log's level <see cref="Level"/></param>
        /// <param name="format">log's format. a string that represent arguments format</param>
        /// <param name="args">log's arguments</param>
        public void Log(Level level, string format, params object[] args) { Log(level, string.Format(format, args)); }
        /// <summary>
        /// Writes Debug message with specified format.
        /// </summary>
        /// <param name="format">log's format. a string that represent arguments format</param>
        /// <param name="args">log's arguments</param>
        public void Debug(string format, params object[] args) { Log(Level.Debug, format, args); }
        /// <summary>
        /// Writes Info message with specified format.
        /// </summary>
        /// <param name="format">log's format. a string that represent arguments format</param>
        /// <param name="args">log's arguments</param>
        public void Info(string format, params object[] args) { Log(Level.Info, format, args); }
        /// <summary>
        /// Writes warning message with specified format.
        /// </summary>
        /// <param name="format">log's format. a string that represent arguments format</param>
        /// <param name="args">log's arguments</param>
        public void Warn(string format, params object[] args) { Log(Level.Warn, format, args); }
        /// <summary>
        /// Writes error message with specified format.
        /// </summary>
        /// <param name="format">log's format. a string that represent arguments format</param>
        /// <param name="args">log's arguments</param>
        public void Error(string format, params object[] args) { Log(Level.Error, format, args); }
        /// <summary>
        /// Writes Fatal message with specified format.
        /// </summary>
        /// <param name="format">log's format. a string that represent arguments format</param>
        /// <param name="args">log's arguments</param>
        public void Fatal(string format, params object[] args) { Log(Level.Fatal, format, args); }
        /// <summary>
        /// Writes log from an obejct.
        /// </summary>
        /// <param name="level">log's piority level. <see cref="Level"/></param>
        /// <param name="obj">an Object with required parametes. Convertation to String is made with .ToString() method.</param>
        public void Log(Level level, object obj) { Log(level, obj.ToString()); }
        /// <summary>
        /// Writes Debug log from reffered object.
        /// </summary>
        /// <param name="obj">an Object with required parametes.</param>
        public void Debug(object obj) { Log(Level.Debug, obj); }
        /// <summary>
        /// Writes Info log from reffered object.
        /// </summary>
        /// <param name="obj">an Object with required parametes.</param>
        public void Info(object obj) { Log(Level.Info, obj); }
        /// <summary>
        /// Writes warning log from reffred object.
        /// </summary>
        /// <param name="obj">an Object with required parametes.</param>
        public void Warn(object obj) { Log(Level.Warn, obj); }
        /// <summary>
        /// Writes Error log from reffered object.
        /// </summary>
        /// <param name="obj">an Object with required parametes.</param>
        public void Error(object obj) { Log(Level.Error, obj); }
        /// <summary>
        /// Writes Fatal log from reffered object.
        /// </summary>
        /// <param name="obj">an Object with required parametes.</param>
        public void Fatal(object obj) { Log(Level.Fatal, obj); }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~Logger() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
