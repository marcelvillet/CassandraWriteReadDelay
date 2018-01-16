using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraWriteReadDelay
{
    class Program
    {
        #region Fields and Properties

        const int NO_TABLES = 5;
        const int TOTAL_NO_TASKS = 100000;
        const int TOTAL_NO_THREADS = 20;

        static Random Random = new Random(123);
        static ISession Session { get; set; }
        static PreparedStatement[] _psInsert = null;
        static PreparedStatement[] PsInsert
        {
            get
            {
                if (_psInsert == null)
                {
                    _psInsert = new PreparedStatement[NO_TABLES];

                    for (int i = 0; i < NO_TABLES; i++)
                    {
                        _psInsert[i] = Session.Prepare($"INSERT INTO test{i} (id0, id1, id2, id3, id4, id5, id6, id7, id8, id9, name, csharpDate, cqlDate, tuuid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, toUnixTimestamp(now()), now())");
                        _psInsert[i].SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                    }
                }

                return _psInsert;
            }
        }
        static PreparedStatement[] _psSelect = null;
        static PreparedStatement[] PsSelect
        {
            get
            {
                if (_psSelect == null)
                {
                    _psSelect = new PreparedStatement[NO_TABLES];

                    for (int i = 0; i < NO_TABLES; i++)
                    {
                        _psSelect[i] = Session.Prepare($"SELECT * FROM test{i} WHERE id{i} = ?");
                        _psSelect[i].SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                    }
                }

                return _psSelect;
            }
        }
        
        static int NoTasksExecuted = 0;

        #endregion

        #region Main

        /// <summary>
        /// Command line arguments:
        /// 
        /// ip
        /// ip username password
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            // --------------------------------------------------------------------------------------------------------
            // Connect
            // --------------------------------------------------------------------------------------------------------

            string k = "test1";
            string ip = args[0];
            string u = args.Length > 1 ? args[1] : "dummy";
            string p = args.Length > 1 ? args[2] : "dummy";

            //Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;
            //Trace.Listeners.Add(new ConsoleTraceListener());

            WriteLine("Connecting to Cassandra...");

            Builder builder = Cluster.Builder()
                .AddContactPoints(ip)
                .WithCompression(CompressionType.Snappy)
                .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                //.WithPoolingOptions(options)
                .WithCredentials(u, p);

            var cluster = builder.Build();
            Session = cluster.Connect();

            WriteLine("Connected");

            // --------------------------------------------------------------------------------------------------------
            // Setup
            // --------------------------------------------------------------------------------------------------------

            Session.DeleteKeyspaceIfExists(k);
            Session.CreateKeyspace(k);
            Session.Execute($"USE {k}");

            for (int i = 0; i < NO_TABLES; i++)
            {
                var tableName = $"test{i}";
                var tableDropCql = $"DROP TABLE IF EXISTS {tableName}";
                var tableCreateCql = @"
CREATE TABLE test" + i + @" (
    id0          int,
    id1          int,
    id2          int,
    id3          int,
    id4          int,
    id5          int,
    id6          int,
    id7          int,
    id8          int,
    id9          int,
    name         text,
    csharpDate   timestamp,
    cqlDate      timestamp,
    tuuid        timeuuid,

    primary key (id" + i + @")
);";

                //WriteLine($"Dropping table {tableName}...");

                //Session.Execute(tableDropCql);

                //WriteLine("Dropped");

                WriteLine($"Creating table {tableName}...");

                Session.Execute(tableCreateCql);

                WriteLine("Created");
            }

            WriteLine("Creating prepared statements...");

            var psInsert = PsInsert;
            var psSelect = PsSelect;

            WriteLine("Created");

            WriteLine("Running test...");

            var exit = false;
            var thread = new Thread(new ThreadStart(() =>
            {
                var nextPct = 1;

                while(!exit)
                {
                    var currPct = (int)(100.0 * NoTasksExecuted / TOTAL_NO_TASKS);

                    if (currPct > nextPct)
                    {
                        WriteLine($"{currPct}% completed");
                        nextPct = currPct;
                    }

                    Thread.Sleep(100);
                }
            }));

            thread.IsBackground = true;
            thread.Start();

            Stopwatch sw = Stopwatch.StartNew();
            RunThreads(TOTAL_NO_THREADS, TOTAL_NO_TASKS);
            sw.Stop();
            //RunTasks(0, TotalNoTasks);

            exit = true;
            thread.Join();

            WriteLine("Done");
            WriteLine($"{TOTAL_NO_TASKS} tasks executed in {sw.Elapsed} using {TOTAL_NO_THREADS} threads");
            WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void WriteLine(string s)
        {
            Console.WriteLine(string.Format("{0} - {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fffffff"), s));
        }

        private static async Task PerformIO(int index)
        {
            // Perform insert
            BatchStatement batch = new BatchStatement();

            batch.SetBatchType(BatchType.Logged);

            for (int i = 0; i < NO_TABLES; i++)
            {
                byte[] bytes = new byte[1000];

                Random.NextBytes(bytes);

                var name = Convert.ToBase64String(bytes);
                var bs = PsInsert[i].Bind(index, index + 1, index + 2, index + 3, index + 4, index + 5, index + 6, index + 7, index + 8, index + 9, name, DateTime.UtcNow);

                batch.Add(bs);
            }

            await Session.ExecuteAsync(batch);

            // Perform select
            for (int i = 0; i < NO_TABLES; i++)
            {
                var bs = PsSelect[i].Bind(i);
                var rowSet = await Session.ExecuteAsync(bs);
                var row = rowSet.FirstOrDefault();

                if (row == null)
                {
                    Console.WriteLine("Select failed for index " + index);
                }
            }

            Interlocked.Increment(ref NoTasksExecuted);
        }

        private static void RunTasks(int indexStart, int noTasks)
        {
            for (int i = 0; i < noTasks; i++)
            {
                int index = indexStart + i;
                PerformIO(index).Wait();
            }
        }

        private static void RunThreads(int noThreads, int noTasks)
        {
            Thread[] threads = new Thread[noThreads];
            var noTasksPerThread = noTasks / noThreads;

            for (int i = 0; i < noThreads; i++)
            {
                var indexStart = i * noTasksPerThread;
                threads[i] = new Thread(new ThreadStart(() => RunTasks(indexStart, noTasksPerThread)));
                threads[i].Start();
            }

            for (int i = 0; i < noThreads; i++)
            {
                threads[i].Join();
            }
        }

        #endregion
    }

}
