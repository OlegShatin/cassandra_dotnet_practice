using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Cassandra;

namespace CassandraNETRun
{
    [ClrJob(baseline: true)]
    [RPlotExporter, RankColumn]
    public class FlightsCassandraBenchs
    {
        protected string[] InsertQueries;
        protected string[] SelectQueries;
        protected ISession session;
        [Params(1000)]
        public int N;

        [GlobalSetup]
        public void Setup()
        {
            var rand = new Random(100);
            InsertQueries = new string[N];
            SelectQueries = new string[N];
            var lettersNum = 4;
            for (int i = 0; i < N; i++)
            {
                var flight = (
                        Id: i,
                        From: ("" + (char)('A' + rand.Next(lettersNum)) + (char)('A' + rand.Next(lettersNum)) + (char)('A' + rand.Next(lettersNum))),
                        To: ("" + (char)('A' + rand.Next(lettersNum)) + (char)('A' + rand.Next(lettersNum)) + (char)('A' + rand.Next(lettersNum))),
                        Cost: rand.Next(900)
                    );
                InsertQueries[i] = "INSERT INTO flights (fl_id, fl_from, fl_to, fl_cost) VALUES ("
                             + flight.Id + ", '" + flight.From + "', '" + flight.To + "', " + flight.Cost + ");";
                SelectQueries[i] = "SELECT * FROM flights WHERE " +
                                   "fl_from = '" + flight.From + "' AND  fl_to = '" + flight.To + "';";
            }
            var cluster = Cluster.Builder()
                .AddContactPoints("127.0.0.1")
                .Build();
            // Connect to the nodes using a keyspace
            session = cluster.Connect("airtickets");
            // Execute a query on a connection synchronously
            //var rs = session.Execute("SELECT * FROM emp;");
        }

        

        [Benchmark(OperationsPerInvoke = 1000)]
        public async Task FlightsAsyncInsert()
        {
            var tasks = new Task[N];
            for (int i = 0; i < N; i++)
            {
                tasks[i] = session.ExecuteAsync(new SimpleStatement(InsertQueries[i]));
            }

            await Task.WhenAll(tasks);
        }

        [Benchmark(OperationsPerInvoke = 1000)]
        public async Task FlightsAsyncSelect()
        {
            var tasks = new Task[N];
            for (int i = 0; i < N; i++)
            {
                tasks[i] = session.ExecuteAsync(new SimpleStatement(SelectQueries[i]));
            }

            await Task.WhenAll(tasks);
        }

        [GlobalCleanup]
        public void Clear()
        {
            session.Execute("truncate flights;");
            session.Dispose();
        }
    }
}
