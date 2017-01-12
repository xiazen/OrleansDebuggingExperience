using System;
using System.IO;
using System.Security;
using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;

namespace OrleansStatisticsVisualization
{

    public class BasicFileLogger
    {
        public BasicFileLogger(string logFileName)
        {
            try
            {
                // Open the file
                this.writer = TextWriter.Synchronized(System.IO.File.AppendText(logFileName));
            }
            catch (Exception ex)
            {
                if
                (
                    ex is UnauthorizedAccessException
                    || ex is ArgumentNullException
                    || ex is PathTooLongException
                    || ex is DirectoryNotFoundException
                    || ex is NotSupportedException
                    || ex is ArgumentException
                    || ex is SecurityException
                    || ex is IOException
                )
                {
                    throw new LoggerException("Failed to create log file: " + ex.Message);
                }
                else
                {
                    // Unexpected failure
                    throw;
                }
            }
        }
            /// <summary>
            /// Just write a line to the log
            /// </summary>
        public void WriteLine(string line)
        {
            writer.WriteLine(String.Format("{0}: {1}", DateTime.UtcNow, line));
            Console.WriteLine(String.Format("{0}: {1}", DateTime.UtcNow, line));
        }

        public void WriteToConsole(string line)
        {
            Console.WriteLine(String.Format("{0}: {1}", DateTime.UtcNow, line));
        }

        /// <summary>
        /// Shutdown() is guaranteed to be called by MSBuild at the end of the build, after all 
        /// events have been raised.
        /// </summary>
        public void Shutdown()
        {
            // Done logging, let go of the file
            writer.Close();
        }

        private TextWriter writer;
    }

}