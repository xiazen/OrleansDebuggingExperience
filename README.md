# OrleansDebuggingExperience

How to use OrleansStatisticsVisualization?

1. Pick the emails of the load tests builds you want to visualize, save it to a file. One LoadTestType at a time. Don't support uploading multiple load test type builds yet.

2. Build OrleansStatisticVisualizatio proj

3. Run OrleansStatisticsVisualization.exe emailFileLocation LoadTestType 



It would generate 6 azure table in your account: 

1. 'LoadTestType'AppRequestLatencyHistogramTableForClient

2. 'LoadTestType'AppRequestLatencyHistogramTableForSilo

3. 'LoadTestType'ClientStatistics

4. 'LoadTestType'SiloStatistics

5. 'LoadTestType'TestResultMetrics -- store TPS metrics which are original stored in the load test email

6. 'LoadTestType'TPSRelatedMetrics -- store raw TPS metrics which are used to calculate AggregatedTPS and AggregatedTPSMoving: in each reporting period(from last report timestamp to current report timestamp), how many TPS does this each test app client processed. 
