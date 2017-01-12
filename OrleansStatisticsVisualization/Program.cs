using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount

namespace OrleansStatisticsVisualization
{
    class Program
    {
        static void Main(string[] args)
        {
            var now = System.DateTime.UtcNow;
            BasicFileLogger logger = new BasicFileLogger("LogFile-" + now.Year + '-' + now.Month + '-' + now.Day + '-' + now.Hour + '-' + now.Minute + ".log");
            LoadTestFolderLocator locator = new LoadTestFolderLocator(logger);
            var testResultFolderList = locator.GetLoadTestResultFolderListFromEmailText(args[0]);
            LogToStatisticsUploader statisticUploader = new LogToStatisticsUploader(logger);
            foreach (string testResultFolder in testResultFolderList)
            {
                statisticUploader.UploadStatisticsToAzureTable(testResultFolder, args[1]);
            }
            
            logger.Shutdown();
        }
    }
}
