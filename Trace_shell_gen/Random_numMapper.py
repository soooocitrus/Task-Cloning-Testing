import csv
import random

pi_command = "hadoopc jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar pi"
teragen_command = "hadoopc jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar teragen -D mapred.map.tasks="
wc_command = "hadoopc jar ~/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.4.jar wordcount"

pi_map = 1
pi_sample = 10000000
pi_coeff = float(750000000/11)
pi_intercept = -840909086

teragen_map = 1
teragen_size = 45000000
teragen_outdir = "teragen/"
teragen_count = 0
teragen_coeff = 598827.47
teragen_intercept = 14752931.32

wc_indir = "wordcount_input/*"
wc_outdir = "wc_output/"
wc_count = 0
last_time=0
with open('jobfileF18.csv','wb') as outfile:
    writer = csv.writer(outfile, delimiter=',')
    with open('job_file.csv','rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for index,row in enumerate(spamreader):
            if row[1] == '1':
                row[1] = str(random.randint(1,10))
            writer.writerow(row)
