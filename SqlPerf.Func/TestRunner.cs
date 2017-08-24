using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs.Host;

namespace SqlPerf.Func
{
    public class TestRunner
    {
        public async Task<HttpResponseMessage> TestAll(HttpRequestMessage req, TraceWriter log)
        {
            int count = int.Parse(req.GetQueryNameValuePairs()
                                      .FirstOrDefault(q => string.Compare(q.Key, "count", true) == 0)
                                      .Value ?? "50000");
            int repeat = int.Parse(req.GetQueryNameValuePairs()
                                       .FirstOrDefault(q => string.Compare(q.Key, "repeat", true) == 0)
                                       .Value ?? "1");
            int batchSize = int.Parse(req.GetQueryNameValuePairs()
                                          .FirstOrDefault(q => string.Compare(q.Key, "batchSize", true) == 0)
                                          .Value ?? "50000");

            var connection = ConfigurationManager.AppSettings["TargetDatabase"];

            var simpleTable = await RunTest(connection, "SimpleTable", CreateSimpleTable, count, repeat, batchSize, log);
            var simpleTableNoKey = await RunTest(connection, "SimpleTable_NoKey", CreateSimpleTable, count, repeat, batchSize, log);
            var complexTable = await RunTest(connection, "ComplexTable", CreateComplexTable, count, repeat, batchSize, log);
            var complexTableNoKey = await RunTest(connection, "ComplexTable_NoKey", CreateComplexTable, count, repeat, batchSize, log);

            return req.CreateResponse(HttpStatusCode.Accepted, new
            {
                simpleTable,
                simpleTableNoKey,
                complexTable,
                complexTableNoKey,
            });
        }

        public async Task<HttpResponseMessage> RunTest(string table, Func<int, int, DataTable> builder, HttpRequestMessage req, TraceWriter log)
        {
            int count = int.Parse(req.GetQueryNameValuePairs()
                                      .FirstOrDefault(q => string.Compare(q.Key, "count", true) == 0)
                                      .Value ?? "50000");
            int repeat = int.Parse(req.GetQueryNameValuePairs()
                                      .FirstOrDefault(q => string.Compare(q.Key, "repeat", true) == 0)
                                      .Value ?? "1");
            int batchSize = int.Parse(req.GetQueryNameValuePairs()
                                      .FirstOrDefault(q => string.Compare(q.Key, "batchSize", true) == 0)
                                      .Value ?? "50000");

            var connection = ConfigurationManager.AppSettings["TargetDatabase"];
            var results = await RunTest(connection, table, builder, count, repeat, batchSize, log);

            return req.CreateResponse(HttpStatusCode.Accepted, results);
        }

        public async Task<ICollection<TimeSpan[]>> RunTest(string connection, string table, Func<int, int, DataTable> builder, int count, int repeat, int batchSize, TraceWriter log)
        {
            var results = new List<TimeSpan[]>();
            await ClearTable(connection, table);
            for (var i = 0; i < repeat; i++)
            {
                var dataSw = Stopwatch.StartNew();
                var data = builder(count, i * count);
                var dataTime = dataSw.Elapsed;
                log.Info($"Created data in {dataTime}");
                var insertSw = Stopwatch.StartNew();
                await BulkInsert(connection, table, data, batchSize);
                var insertTime = insertSw.Elapsed;
                log.Info($"Inserted data in {insertTime}");
                results.Add(new[] { dataTime, insertTime });
            }
            return results;
        }

        public async Task ClearTable(string connection, string table)
        {
            using (var cn = new SqlConnection(connection))
            {
                await cn.OpenAsync();
                var cmd = cn.CreateCommand();
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = "truncate table " + table;
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task BulkInsert(string connection, string table, DataTable data, int batchSize)
        {
            var sbc = new SqlBulkCopy(connection)
            {
                DestinationTableName = table,
                BatchSize = batchSize,
                BulkCopyTimeout = 120,
            };

            await sbc.WriteToServerAsync(data);
        }

        public DataTable CreateSimpleTable(int rows, int start)
        {
            var dataTable = new DataTable();
            dataTable.Columns.Add("IntField", typeof(int));
            dataTable.Columns.Add("StringField", typeof(string));
            dataTable.Columns.Add("StringField1", typeof(string));
            dataTable.Columns.Add("DecimalField", typeof(decimal));

            for (var i = 0; i < rows; i++)
            {
                dataTable.Rows.Add(new object[] { (start + i), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), (decimal)rows });
            }
            return dataTable;
        }

        public DataTable CreateComplexTable(int rows, int start)
        {
            var dataTable = new DataTable();
            dataTable.Columns.Add("StringField", typeof(string));
            dataTable.Columns.Add("StringField2", typeof(string));
            dataTable.Columns.Add("StringField3", typeof(string));
            dataTable.Columns.Add("StringField4", typeof(string));
            dataTable.Columns.Add("StringField5", typeof(string));
            dataTable.Columns.Add("StringField6", typeof(string));
            dataTable.Columns.Add("StringField7", typeof(string));

            for (var i = 0; i < rows; i++)
            {
                dataTable.Rows.Add(new object[] {
                    (start + i).ToString(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                    Guid.NewGuid().ToString(),
                });
            }
            return dataTable;
        }
        // Define other methods and classes here

    }
}
