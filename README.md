# Word-Occurrence-Count-in-Monthly-Crawls-using-Spark
The project involved processing a large amount of web data to count the occurrence of the word "bear" in a monthly crawl segment. The project was implemented using Apache Spark, a distributed computing system, to analyze the data in parallel across multiple machines.

The project consisted of two main parts: data processing and analysis. In the data processing part, the web data was read from a WARC file using the WarcGzInputFormat and WarcWritable classes, and then cleaned and filtered using regular expressions to remove any special characters or unwanted words. The cleaned data was then mapped to count the occurrences of the target word using Spark's map-reduce functionality. Finally, the results were aggregated to produce a count of the word's occurrence.

The analysis was performed on the monthly crawl segment provided, and the final version of the program was able to complete the task in 25.3 hours. However, the program faced some challenges during its development, such as issues with input path and out of memory errors. The program was eventually optimized by tweaking the Spark configuration and adjusting the resource allocation to avoid these issues.

Overall, this project demonstrates how Spark can be used to process and analyze large datasets efficiently and in a distributed manner, and how it can be used to solve real-world data analysis problems.
