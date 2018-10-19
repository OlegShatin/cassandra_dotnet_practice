using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Cassandra;

namespace CassandraNETRun
{
    class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<FlightsCassandraBenchs>();
            Console.ReadKey();
        }

        private static void SimpleConsole()
        {
            Console.WriteLine("Hello World!");

            var cluster = Cluster.Builder()
                .AddContactPoints("127.0.0.1")
                .Build();
            // Connect to the nodes using a keyspace
            var session = cluster.Connect("dev");
            // Execute a query on a connection synchronously
            var rs = session.Execute("SELECT * FROM emp;");
            // Iterate through the RowSet
            foreach (var row in rs)
            {
                var value = row.GetValue<string>("emp_last");
                // Do something with the value
                Console.WriteLine(value);
            }

            Console.ReadKey();
        }
    }
    [ClrJob(baseline:true)]
    [RPlotExporter, RankColumn]
    public class CassandraTestBench
    {
        protected string[] queries;
        protected ISession session;
        [Params(1000)]
        public int N;

        [GlobalSetup]
        public void Setup()
        {
            var rand = new Random(100);
            queries = new string[N];
            for (int i = 0; i < N; i++)
            {
                queries[i] = "INSERT INTO emp (empid, emp_dept, emp_first, emp_last) VALUES ("
                             + i + ", 'dept" + rand.Next(100) + "', 'FName" + rand.Next(100) + "', 'LName" + rand.Next(100) + "');";
            }
            var cluster = Cluster.Builder()
                .AddContactPoints("127.0.0.1")
                .Build();
            // Connect to the nodes using a keyspace
            session = cluster.Connect("dev");
            // Execute a query on a connection synchronously
            //var rs = session.Execute("SELECT * FROM emp;");
        }

        [Benchmark]
        public void CassandraSyncInsert()
        {
            for (int i = 0; i < N; i++)
            {
                session.Execute(queries[i]);
            }
        }

        [Benchmark]
        public async Task CassandraAsyncInsert()
        {
            var tasks = new Task[N];
            for (int i = 0; i < N; i++)
            {
                tasks[i] = session.ExecuteAsync(new SimpleStatement(queries[i]));
            }

            await Task.WhenAll(tasks);
        }

        [GlobalCleanup]
        public void Clear()
        {
            session.Execute("truncate emp;");
        }
    }

    
}
