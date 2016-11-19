using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Blob storage types

namespace AzureUtils
{
    class AzureBlobDAO
    {
        private CloudBlobClient cloudBlobClient;
        private static string TestContainerName = "OrleansLoadTestResults";
        private static string OrleansBuildNo3DirectorySearchPattern = "xcgbuild-ORLEANS-BUILD-3*";
        public AzureBlobDAO(CloudStorageAccount cloudStorageAccount)
        {
            cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
        }

        public void UploadTestResultsToBlobContainer(string testFolderPath)
        {
            CloudBlobContainer container = cloudBlobClient.GetContainerReference(TestContainerName);
            container.CreateIfNotExists();
            // all the folders name match pattern xcgbuild-ORLEANS-BUILD-3*
            var orleansBuildNo3TestResultDirs = System.IO.Directory.EnumerateDirectories(testFolderPath, OrleansBuildNo3DirectorySearchPattern);
            foreach (var buildNo3TestDir in orleansBuildNo3TestResultDirs)
            {
                string topFolderName = buildNo3TestDir.Substring(buildNo3TestDir.LastIndexOf("\\") + 1);
                CloudBlobDirectory topBlobDirectory = container.GetDirectoryReference(topFolderName);
                var testResultFoldersInBuildNo3TestDir = System.IO.Directory.EnumerateDirectories(buildNo3TestDir);
                foreach (var testResultFolder in testResultFoldersInBuildNo3TestDir)
                {
                    string testFolderName = testResultFolder.Substring(testResultFolder.LastIndexOf("\\") + 1);
                    CloudBlobDirectory testResultBlobDirectory = topBlobDirectory.GetDirectoryReference(testFolderName);
                    foreach (var filePath in System.IO.Directory.EnumerateFiles(testResultFolder))
                    {
                        string fileName = filePath.Substring(filePath.LastIndexOf("\\" + 1));
                        CloudBlockBlob fileBlob = testResultBlobDirectory.GetBlockBlobReference(fileName);
                        fileBlob.UploadFromFile(filePath);
                    }
                }

            }
        }
    }
}
