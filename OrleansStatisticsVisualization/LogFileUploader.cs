using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Blob storage types

namespace OrleansStatisticsVisualization
{
    class AzureBlobDAO
    {
        private CloudBlobClient cloudBlobClient;
        private static string TestContainerName = "orleansloadtestresults";
        private static string OrleansBuildNo3 = "xcgbuild-ORLEANS-BUILD-3";
        private static string testSavedLogsFolderPath = "D:\\testResults";
        private BasicFileLogger logger;
        public AzureBlobDAO(CloudStorageAccount cloudStorageAccount, BasicFileLogger logger)
        {
            cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            this.logger = logger;
        }

        public void UploadTestResultsToBlobContainer()
        {
            CloudBlobContainer container = cloudBlobClient.GetContainerReference(TestContainerName);
            container.CreateIfNotExists();
            logger.WriteToConsole("Create blob" + TestContainerName);
            uploadFilesToAzureBlob(container, testSavedLogsFolderPath, OrleansBuildNo3);
        }

        private void uploadFilesToAzureBlob(CloudBlobContainer container, string testSavedLogsFolderPath, string buildName)
        {
            string searchPattern = buildName + "*";
            var testResultDirs = System.IO.Directory.EnumerateDirectories(testSavedLogsFolderPath, searchPattern);
            CloudBlobDirectory buildFolder = container.GetDirectoryReference(buildName);
            foreach (var testResultDir in testResultDirs)
            {
                string topFolderName = testResultDir.Substring(testResultDir.LastIndexOf("\\") + 1);
                CloudBlobDirectory topBlobDirectory = buildFolder.GetDirectoryReference(topFolderName);
                logger.WriteToConsole("create a top test directory " + topFolderName);
                var testResultFolders = System.IO.Directory.EnumerateDirectories(testResultDir);
                foreach (var testResultFolder in testResultFolders)
                {
                    string testFolderName = testResultFolder.Substring(testResultFolder.LastIndexOf("\\") + 1);
                    CloudBlobDirectory testResultBlobDirectory = topBlobDirectory.GetDirectoryReference(testFolderName);
                    logger.WriteToConsole("create a test run folder " + testFolderName);
                    foreach (var filePath in System.IO.Directory.EnumerateFiles(testResultFolder))
                    {
                        string fileName = filePath.Substring(filePath.LastIndexOf("\\") + 1);
                        CloudBlockBlob fileBlob = testResultBlobDirectory.GetBlockBlobReference(fileName);
                        logger.WriteToConsole("Create a file blob " + fileName);
                        fileBlob.UploadFromFile(filePath);
                    }
                }

            }
        }
    }
}
