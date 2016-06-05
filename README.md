# CompanyRecommendation
![image](https://github.com/ycl11761/CompanyRecommendation/blob/master/mapreduce.png)</br>
##Problem Description##</br>
Suppose there is a graph for company connections, where each node represents a company (eg.
C1, C2 …) and each edge represents exiting connection/business between two companies. Suppose
companies want to expand their business. They are looking for a MapReduce application to find
companies which they don’t direct connection to but are only 2 hops away in the graph. In the
following, we will call them 2-hop companies. Moreover, for a company C, the connectivity of
its 2-hop company is defined to be the number of distinct 2-hop paths between them.</br>
##Requirements:##</br>
1. For each company, recommend 2-hop companies.</br>
2. Output should be sorted in the descending order of connectivity. If there is a tie, the
node with smaller ID wins.</br>
