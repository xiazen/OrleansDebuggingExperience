using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading;
using System.IO;

namespace OrleansStatisticsVisualization
{
    public class LogToStatisticsUploader
    {
        static string siloLogStartWith = "Silo";
        static string clientLogStartWith = "Client";
        static string tpsMetricFileStartWith = "MetricDefinition";
        static string testAppLogFileStartWith = "Test_";
        private CloudTableClient tableClient;
        private BasicFileLogger logger;
        private int failedRecords;
        public LogToStatisticsUploader(BasicFileLogger logger)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse("connection string");

            tableClient = storageAccount.CreateCloudTableClient();
            this.logger = logger;
            failedRecords = 0;
        }

        public void UploadStatisticsToAzureTable(string testFolderPath, string testType)
        {
            var siloStatisticTable = tableClient.GetTableReference(testType + "SiloStatistics");
            siloStatisticTable.CreateIfNotExists();
            var clientStatisticTable = tableClient.GetTableReference(testType + "ClientStatistics");
            clientStatisticTable.CreateIfNotExists();
            var tpsMetricTable = tableClient.GetTableReference(testType + "TestResultMetrics");
            tpsMetricTable.CreateIfNotExists();
            var siloAppRequestLatencyHisogramTable = tableClient.GetTableReference(testType + "AppRequestLatencyHisogramTableForSilo");
            siloAppRequestLatencyHisogramTable.CreateIfNotExists();
            var clientAppRequestLatencyHisogramTable = tableClient.GetTableReference(testType + "AppRequestLatencyHisogramTableForClient");
            clientAppRequestLatencyHisogramTable.CreateIfNotExists();
            var tpsMetricsFromTestAppLogsTable = tableClient.GetTableReference(testType + "TPSRelatedMetrics");
            tpsMetricsFromTestAppLogsTable.CreateIfNotExists();
            bool alreadyParsedTPS = false;
            foreach (var filePath in System.IO.Directory.EnumerateFiles(testFolderPath))
            {
                string fileName = filePath.Substring(filePath.LastIndexOf("\\") + 1);
                string deploymentId;
                if (fileName.StartsWith(siloLogStartWith))
                {
                    logger.WriteLine("Start to upload silo statistics from file" + filePath);
                    deploymentId = UploadLogStatistic(filePath, siloStatisticTable, siloAppRequestLatencyHisogramTable, true);
                    logger.WriteLine(String.Format("DeploymentId for {0} is {1}", filePath, deploymentId));
                    if (!alreadyParsedTPS)
                    {
                        alreadyParsedTPS = true;
                        foreach (var filePath2 in System.IO.Directory.EnumerateFiles(testFolderPath))
                        {
                            string fileName2 = filePath2.Substring(filePath.LastIndexOf("\\") + 1);
                            if (fileName2.StartsWith(tpsMetricFileStartWith))
                            {
                                logger.WriteLine("Start to upload load test results(TPS metrics) from file" + filePath);
                                UploadTPSMetrics(filePath2, deploymentId, tpsMetricTable);
                            }
                        }
                    }
                }
                else if (fileName.StartsWith(clientLogStartWith))
                {
                    logger.WriteLine("Start to upload client statistics from file" + filePath);
                    UploadLogStatistic(filePath, clientStatisticTable, clientAppRequestLatencyHisogramTable, false);
                }
                else if (fileName.StartsWith(testAppLogFileStartWith))
                {
                    logger.WriteLine("Start to upload tps related metrics from file" + filePath);
                    UploadTPSFromLogFiles(filePath, tpsMetricsFromTestAppLogsTable);
                }
            }

            logger.WriteLine("LogToStatisticsUploader : failed uploading records count : " + failedRecords);
        }

        static string AggrTPSMetricName = "Aggregate of TPS";
        static string AggrMovingTPSMetricName = "Aggregate of TPS Moving";
        #region UploadDifferentMetrics
        void UploadTPSFromLogFiles(string filePath, CloudTable tableRef)
        {
            var entityMap = new Dictionary<String, List<DynamicTableEntity>>();
            string[] lines = File.ReadAllLines(filePath);
            string name = "";
            string deploymentId = "";
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                if (line.Contains("DeploymentId: "))
                {
                    int index = line.IndexOf("DeploymentId: ");
                    deploymentId = line.Substring(index + "DeploymentId: ".Length);
                }
                if (line.Contains("Client Name: "))
                {
                    name = line.Substring(line.IndexOf("Client Name: ") + "Client Name: ".Length);
                }
                if (line.Contains("Current TPS: ")&&line.Contains("PipeSize: "))
                {
                    DynamicTableEntity entity = ParseTPSMetricsFromLine(line, deploymentId, name);
                    if (entity != null)
                    {
                        List<DynamicTableEntity> entityList;
                        if (entityMap.TryGetValue(entity.PartitionKey, out entityList))
                        {
                            entityList.Add(entity);
                        }
                        else
                        {
                            entityList = new List<DynamicTableEntity>();
                            entityList.Add(entity);
                            entityMap.Add(entity.PartitionKey, entityList);
                        }
                    }
                }
            }

            var bulkPromises = ConvertEntityMapToBatchOperationPromises(entityMap, tableRef);
            Task.WhenAll(bulkPromises).Wait();
        }

        
        void UploadTPSMetrics(string filePath, string deploymentId, CloudTable tableRef)
        {
            List<Task> bulkPromises = new List<Task>();
            List<Double> aggrTPSValues = new List<Double>();
            List<Double> aggrMovingTPSValues = new List<Double>();
            int aggrTPSValuesIndex = -1;
            int aggrTPSMovingTPSValuesIndex = -1;
            CsvFileReader reader = new CsvFileReader(filePath);
            CsvRow row = new CsvRow();
            while (reader.ReadRow(row))
            {
                for(int i = 0; i < row.Count; i++)
                {
                    string s = row[i];
                    if (s.Equals(AggrMovingTPSMetricName))
                    {
                        aggrTPSMovingTPSValuesIndex = i;
                    }
                    else if (s.Equals(AggrTPSMetricName))
                    {
                        aggrTPSValuesIndex = i;
                    }
                    else if (i == aggrTPSMovingTPSValuesIndex)
                    {
                        double re;
                        if (Double.TryParse(s, out re))
                        {
                            aggrMovingTPSValues.Add(re);
                        }
                        
                    }
                    else if (i == aggrTPSValuesIndex)
                    {
                        double re;
                        if (Double.TryParse(s, out re))
                        {
                            aggrTPSValues.Add(re);
                        }
                    }
                }
            }
            
            DateTime loadtestEndingTime = File.GetCreationTime(filePath);
            List<DynamicTableEntity> entities = new List<DynamicTableEntity>();
            for (int i = 0; i < aggrTPSValues.Count; i++)
            {
                if (entities.Count >= AzureTableDefaultPolicies_MAX_BULK_UPDATE_ROWS)
                {
                    bulkPromises.Add(ConvertToBatchOperation(entities, tableRef));
                    entities.Clear();
                }
                DynamicTableEntity entity = new DynamicTableEntity();
                entity.PartitionKey = deploymentId;
                entity.RowKey = deploymentId + "$" + i.ToString();
                entity.Properties.Add(ConvertToProperPropertyName(AggrTPSMetricName), new EntityProperty(aggrTPSValues[i]));
                entity.Properties.Add("Time", new EntityProperty(loadtestEndingTime.ToUniversalTime()));
                if (i < aggrMovingTPSValues.Count)
                {
                    entity.Properties.Add(ConvertToProperPropertyName(AggrMovingTPSMetricName), new EntityProperty(aggrMovingTPSValues[i]));
                }
                entities.Add(entity);
            }
            if (entities.Count > 0)
            {
                bulkPromises.Add(ConvertToBatchOperation(entities, tableRef));
            }
            Task.WhenAll(bulkPromises).Wait();
        }


        string UploadLogStatistic(string filePath, CloudTable statisticTable, CloudTable appRequestLatencyHistogramTable,bool isSilo)
        {
            // key: partitionkey, value, list of DynamicTableEntity which share the same partitionKey, 
            // why? since one azure table batch operation can only process entities share the same partitionKey
            var entityMap = new Dictionary<String, List<DynamicTableEntity>>();
            var appRequestLatencyHostogramMap = new Dictionary<String, List<DynamicTableEntity>>();
            string name = "";
            string deploymentId = "";
            string[] lines = System.IO.File.ReadAllLines(filePath);
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i];
                if (line.Contains("DeploymentId: "))
                {
                    int index = line.IndexOf("DeploymentId: ");
                    deploymentId = line.Substring(index + "DeploymentId: ".Length);
                }
                if (isSilo && line.Contains("Silo Name: "))
                {
                    name = line.Substring(line.IndexOf("Silo Name: ") + "Silo Name: ".Length);
                }
                if (!isSilo && line.Contains("Client Name: "))
                {
                    name = line.Substring(line.IndexOf("Client Name: ") + "Client Name: ".Length);
                }
                if (line.Contains("Statistics: ^^^"))
                {
                    List<DynamicTableEntity> appRequestLatencyHistogramForCurrentLine = new List<DynamicTableEntity>();
                    DynamicTableEntity entity = ParseStatisticLinesToTableEntity(lines, i, deploymentId, name, isSilo, out i, out appRequestLatencyHistogramForCurrentLine);
                    if (entity != null)
                    {
                        List<DynamicTableEntity> entityList;
                        if (entityMap.TryGetValue(entity.PartitionKey, out entityList))
                        {
                            entityList.Add(entity);
                        }
                        else
                        {
                            entityList = new List<DynamicTableEntity>();
                            entityList.Add(entity);
                            entityMap.Add(entity.PartitionKey, entityList);
                        }
                    }
                   
                    foreach (var entity2 in appRequestLatencyHistogramForCurrentLine)
                    {
                        List<DynamicTableEntity> entityList2;
                        if (appRequestLatencyHostogramMap.TryGetValue(entity2.PartitionKey, out entityList2))
                        {
                            entityList2.Add(entity2);
                        }
                        else
                        {
                            entityList2= new List<DynamicTableEntity>();
                            entityList2.Add(entity2);
                            appRequestLatencyHostogramMap.Add(entity2.PartitionKey, entityList2);
                        }
                    }
                    
                }
            }
            
            var bulkPromises = ConvertEntityMapToBatchOperationPromises(entityMap, statisticTable);
            foreach (var promise in ConvertEntityMapToBatchOperationPromises(appRequestLatencyHostogramMap, appRequestLatencyHistogramTable))
            {
                bulkPromises.Add(promise);
            }

            Task.WhenAll(bulkPromises).Wait();
            return deploymentId;
        }
        #endregion
        static int AzureTableDefaultPolicies_MAX_BULK_UPDATE_ROWS = 100;

        #region BatchOperationRelated
        List<Task> ConvertEntityMapToBatchOperationPromises(Dictionary<String, List<DynamicTableEntity>> entities, CloudTable testResultTable)
        {
            List<Task> bulkPromises = new List<Task>();
            foreach (KeyValuePair<String, List<DynamicTableEntity>> pair in entities)
            {
                List<DynamicTableEntity> entityList = pair.Value;

                int index = 0;
                while (index < entityList.Count)
                {
                    List<DynamicTableEntity> entitySubList = entityList.GetRange(index, Math.Min(AzureTableDefaultPolicies_MAX_BULK_UPDATE_ROWS, entityList.Count - index - 1));
                    bulkPromises.Add(ConvertToBatchOperation(entitySubList, testResultTable));
                    index += AzureTableDefaultPolicies_MAX_BULK_UPDATE_ROWS;
                }
            }
            return bulkPromises;
        }

        async Task ConvertToBatchOperation(List<DynamicTableEntity> entities, CloudTable testResultTable)
        {
            var entityBatch = new TableBatchOperation();
            foreach (DynamicTableEntity entry in entities)
            {
                entityBatch.InsertOrReplace(entry);
            }

            try
            {
                logger.WriteLine("LogToStatisticsUploader : Sending batch results to database in a batch");
                await testResultTable.ExecuteBatchAsync(entityBatch);
                logger.WriteLine("LogToStatisticsUploader : Sent batch results to database in a batch succeed.");
            }
            catch (Exception e)
            {
                logger.WriteLine("LogToStatisticsUploader : Encounter exception when trying to send test results to database in a batch, Exception: {0}" + e.ToString());
                logger.WriteLine("LogToStatisticsUploader : fall back to send results one by one");
                foreach (var entity in entities)
                {
                    try
                    {
                        logger.WriteLine("LogToStatisticsUploader : Sending single result to database");
                        await testResultTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));
                        logger.WriteLine("LogToStatisticsUploader : Sent single result to database succeed");
                    }
                    catch (Exception e2)
                    {
                        logger.WriteLine("LogToStatisticsUploader : Encounter exception when trying to send single test results to database, Exception: {0}" + e2.ToString());
                        failedRecords++;
                        logger.WriteLine("LogToStatisticsUploader: failed uploading record count : " + failedRecords);
                    }

                    // throttling
                     Thread.Sleep(100);
                }
            }
        }

        #endregion

        #region StatisticsRelatedParser

        bool StringContainsNumber(string s)
        {
            return s.Any(c => char.IsDigit(c));
        }
        private string ConvertToProperPropertyName(string propertyName)
        {
            return System.Text.RegularExpressions.Regex.Replace(propertyName, "[^a-zA-Z]+", "");
        }

        DynamicTableEntity ParseTPSMetricsFromLine(string line, string deploymentId, string name)
        {
            DynamicTableEntity entity = new DynamicTableEntity();
            //line looks like this :
            //700000, Current TPS: 25438.5, PipeSize: 600, Successes: 50000, Failures: 0, Late: 0, Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            int indexOfComa = line.IndexOf(',');
            double totalRequests = Double.Parse(line.Substring(0, indexOfComa));
            //line looks like: Current TPS: 25438.5, PipeSize: 600, Successes: 50000, Failures: 0, Late: 0, Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            int indexOfNumber = line.IndexOf("Current TPS: ") + "Current TPS: ".Length;
            double currentTPS = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: PipeSize: 600, Successes: 50000, Failures: 0, Late: 0, Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("PipeSize: ") + "PipeSize: ".Length;
            double pipeSize = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: Successes: 50000, Failures: 0, Late: 0, Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("Successes: ") + "Successes: ".Length;
            double successes = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: Failures: 0, Late: 0, Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("Failures: ") + "Failures: ".Length;
            double failures = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: Late: 0, Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("Late: ") + "Late: ".Length;
            double lateResponses = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: Busy: 0, Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("Busy: ") + "Busy: ".Length;
            double busyResponses = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: Block Time: 0:0:1.965, CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("Block Time: ") + "Block Time: ".Length;
            TimeSpan blockTime = TimeSpan.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like: CPU now: 91.5, [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            indexOfComa = line.IndexOf(',');
            indexOfNumber = line.IndexOf("CPU now: ") + "CPU now: ".Length;
            double cpuUsage = Double.Parse(line.Substring(indexOfNumber, indexOfComa - indexOfNumber));
            //line looks like:  [2016-12-30 05:28:47.415 GMT]
            line = line.Substring(indexOfComa + 1);
            int indexOfBraket = line.IndexOf('[');
            DateTime time = DateTime.Parse(line.Substring(indexOfBraket + 1, line.IndexOf(']') - indexOfBraket - 1)).ToUniversalTime();
            //parsing completed
            entity.PartitionKey = string.Join("$", deploymentId, string.Format("{0:d19}", time.Ticks - time.Ticks % TimeSpan.TicksPerHour));
            entity.RowKey = string.Join("$", string.Format("{0:d19}", time.Ticks), name);
            entity.Properties.Add("DeploymentId", new EntityProperty(deploymentId));
            entity.Properties.Add("Time", new EntityProperty(time));
            entity.Properties.Add("ClientName", new EntityProperty(name));
            entity.Properties.Add("TotalRequests", new EntityProperty(totalRequests));
            entity.Properties.Add("CurrentTPS", new EntityProperty(currentTPS));
            entity.Properties.Add("PipeSize", new EntityProperty(pipeSize));
            entity.Properties.Add("Successes", new EntityProperty(successes));
            entity.Properties.Add("Failures", new EntityProperty(failures));
            entity.Properties.Add("Late", new EntityProperty(lateResponses));
            entity.Properties.Add("Busy", new EntityProperty(busyResponses));
            entity.Properties.Add("BlockTimeInMillis", new EntityProperty(blockTime.TotalMilliseconds));
            entity.Properties.Add("CurrentCPU", new EntityProperty(cpuUsage));
            return entity;
        }

        static string AppRequestsLatencyHistogramName = "App.Requests.LatencyHistogram.Millis";
        List<DynamicTableEntity> ParseAppRequestsLatencyHistogram(string line, string deploymentId, string name, DateTime time, bool isSilo)
        {
            List<DynamicTableEntity> entities = new List<DynamicTableEntity>();
            string remainingHistogram = line.Substring((AppRequestsLatencyHistogramName + '=').Length);
            int bucketIndex = 0;
            while (!(String.IsNullOrEmpty(remainingHistogram)||remainingHistogram.Equals(" ")))
            {
                DynamicTableEntity entity = new DynamicTableEntity();
                entity.PartitionKey = string.Join("$", deploymentId, string.Format("{0:d19}", time.Ticks - time.Ticks % TimeSpan.TicksPerHour));
                entity.RowKey = string.Join("$", string.Format("{0:d19}", time.Ticks), name, bucketIndex++.ToString());
                entity.Properties.Add("DeploymentId", new EntityProperty(deploymentId));
                entity.Properties.Add("Time", new EntityProperty(time));
                if (isSilo)
                {
                    entity.Properties.Add("SiloName", new EntityProperty(name));
                }
                else
                {
                    entity.Properties.Add("ClientName", new EntityProperty(name));
                }
                // histogram string looks like- [0.4096:0.8191]=756, [0.8192:1.6383]=46618,
                int indexOfBraket = remainingHistogram.IndexOf('[');
                int indexOfColons = remainingHistogram.IndexOf(':');
                double start = Double.Parse(remainingHistogram.Substring(indexOfBraket + 1, indexOfColons - indexOfBraket - 1));
                remainingHistogram = remainingHistogram.Substring(indexOfColons);
                // histogram string looks like- :0.8191]=756, [0.8192:1.6383]=46618,
                indexOfBraket = remainingHistogram.IndexOf(']');
                indexOfColons = remainingHistogram.IndexOf(':');
                double end = Double.Parse(remainingHistogram.Substring(indexOfColons + 1, indexOfBraket - indexOfColons - 1));
                int indexOfEqual = remainingHistogram.IndexOf('=');
                int indexOfComa = remainingHistogram.IndexOf(',');
                double value = Double.Parse(remainingHistogram.Substring(indexOfEqual + 1, indexOfComa - indexOfEqual - 1));
                entity.Properties.Add("BucketStart", new EntityProperty(start));
                entity.Properties.Add("BucketEnd", new EntityProperty(end));
                entity.Properties.Add("BucketValue", new EntityProperty(value));
                entity.Properties.Add("Bucket", new EntityProperty(String.Format("[{0}, {1}]", start, end)));
                // entity.Properties.Add(String.Format("{0}to{1}", Utils.NumWordsWrapper(start), Utils.NumWordsWrapper(end)).Replace('.', 'o'), new EntityProperty(value));
                entities.Add(entity);
                // histogram string looks like [0.8192:1.6383]=46618,
                remainingHistogram = remainingHistogram.Substring(indexOfComa + 1);
            }
            return entities;
        }

        // lineNum : the line number which contains "Statistics: ^^^", which is the start of statistic reporting
        DynamicTableEntity ParseStatisticLinesToTableEntity(string[] lines, int lineNum, string deploymentId, string name, bool isSilo, 
            out int lastVisitedLine, out List<DynamicTableEntity> appRequestsLatencyHistogramList)
        {
            DateTime time;
            string line = lines[lineNum];
            string timeString = line.Substring(1, "2017-01-03 06:55:20.047 GMT".Length);
            List<DynamicTableEntity> appRequestsLatencyHistograms = new List<DynamicTableEntity>();
            try
            {
                time = DateTime.Parse(timeString).ToUniversalTime();
                lineNum++;
                // construct the entity
                DynamicTableEntity entity = new DynamicTableEntity();
                var ticks = time.Ticks;
                entity.PartitionKey = string.Join("$", deploymentId, string.Format("{0:d19}", ticks - ticks % TimeSpan.TicksPerHour));
                entity.RowKey = string.Join("$", string.Format("{0:d19}", ticks), name);
                if (isSilo)
                {
                    entity.Properties.Add("SiloName", new EntityProperty(name));
                }
                else
                {
                    entity.Properties.Add("ClientName", new EntityProperty(name));
                }
                
                entity.Properties.Add("DeploymentId", new EntityProperty(deploymentId));
                entity.Properties.Add("Time", new EntityProperty(time));
                while (lineNum < lines.Length && lines[lineNum].Contains('='))
                {
                    string statisticsLine = lines[lineNum];
                    int index = statisticsLine.IndexOf('=');
                    string statisticName = statisticsLine.Substring(0, index);
                    if (StringContainsNumber(statisticName))
                    {
                        //if statisticName contains number, then it is some statistic like "Messaging.Pings.Received.S10.197.10.0:11130:221122195.Current"
                        // not output those to azure table yet
                        lineNum++;
                        continue;
                    }
                    if (statisticName.Equals(AppRequestsLatencyHistogramName))
                    {
                        appRequestsLatencyHistograms = ParseAppRequestsLatencyHistogram(statisticsLine, deploymentId, name, time, isSilo);
                        //break;
                        lineNum++;
                        continue;
                    }
                    int comaIndex = statisticsLine.IndexOf(',');
                    double statisticValue;
                    bool parseResult;
                    if (comaIndex >= 0)
                    {
                        parseResult = double.TryParse(statisticsLine.Substring(index + 1, comaIndex - index - 1), out statisticValue);
                    }
                    else
                    {
                        parseResult = double.TryParse(statisticsLine.Substring(index + 1), out statisticValue);
                    }
                    if (parseResult)
                    {
                        entity.Properties.Add(ConvertToProperPropertyName(statisticName), new EntityProperty(statisticValue));
                    }

                    lineNum++;
                }
                appRequestsLatencyHistogramList = appRequestsLatencyHistograms;
                lastVisitedLine = lineNum;
                return entity;
            }
            catch (FormatException e)
            {
                logger.WriteLine(String.Format("Time string {0} parsing exception: {1}", timeString, e));
                lastVisitedLine = lineNum + 1;
                appRequestsLatencyHistogramList = appRequestsLatencyHistograms;
                return null;
            }
        }
#endregion


    }
}
