# Testing on Trace

## Trace Files

`job_file.csv`: Original trace, of which most of jobs require only 1 container

>Job type: 0-Computing pi; 1-TeraGen; 2-WordCount

`job_file_Scaled.csv`: Trace with number of required containers uniformly distributed between 1 and 10 (applied to those originally needing 1), and interval between job arrival time scaled up accordingly

## Scripts

`script_Gen.py`: Generate the test script from the trace file

- To generate script in different cases (i.e. srpt with 0/2 copies, capacity scheduler), just selectively comment out the first lines.

- The job sizes are now computed under the assumption that it has a linear relationship between estimated elapsed time and number of mappers. The coefficients and intercepts are obtained by linear regression over 10 data entries.

- To prevent the output from filling up hard disks, the generated script will delete 40 foremost outputs every 40 submitted jobs (e.g. the moment 80th job is submitted, 1-40th job outputs will be deleted)

- Put the generated scripts under ``ubuntu@dicm:~/Tests`` and run. Remember to retain ssh connection when the script is running.

`result.py`: A simplified script to extract the elapsed time of finished jobs and append them accordingly to the trace file

- Need to first download the `.jhist` jobhistory files in order to the working directory.

- If detailed information of finished jobs is needed, please refer to [JobHistory_Analysis](../JobHistory_Analysis).