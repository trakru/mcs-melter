# mcs-melter
## moving from 'Reshape' to 'Pandas.melt()'

migrating existing R code to help "melt" tables into a long format from the excellent R "Reshape" library
pandas.melt method has similar properties, but the execution is a little more convoluted because we dont have dplyr equivalents

## Updated scope
Now migrating code to work with AWS environments natively (i.e. on a local machine) while using data in S3 buckets

To do: 
* create new s3 bucketsto upload fresh data
* create checks to make sure data is not being re-processed

