using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatisticsVisualization
{
    class LoadTestFolderLocator
    {
        BasicFileLogger logger;
        public LoadTestFolderLocator(BasicFileLogger logger)
        {
            this.logger = logger;
        }

        string logFolderIndentifier = "Log outputs stored in ";
        public List<String> GetLoadTestResultFolderListFromEmailText(string emailTextFile)
        {
            List<String> loadTestResultFolderList = new List<string>();
            string[] lines = System.IO.File.ReadAllLines(emailTextFile);
            logger.WriteLine("LoadTestFolderLocator : Found load test result folder list : ");
            foreach (string line in lines)
            {
                int index = line.IndexOf(logFolderIndentifier);
                if (index >= 0)
                {
                    string testFolderPath = line.Substring(index + logFolderIndentifier.Length + 1);
                    testFolderPath = '\\' + testFolderPath;
                    loadTestResultFolderList.Add(@testFolderPath);
                    logger.WriteLine(testFolderPath);
                }
            }
           
            return loadTestResultFolderList;
        }

    }
}
