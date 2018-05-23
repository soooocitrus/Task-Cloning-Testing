import csv
import os
import json

img_dir = "./"
list_of_imgs = []
listTime = []
for img in os.listdir(img_dir):
    if img.endswith('.jhist'):
        list_of_imgs.append(img)

for filename in list_of_imgs:
    with open(filename) as infile:
        for i, line in enumerate(infile):
            if i == 2:
                p = json.loads(line)
                startTime = (
                    p['event']['org.apache.hadoop.mapreduce.jobhistory.AMStarted']['startTime'])
        p = json.loads(line)
        try:
            finishTime = (
                p['event']['org.apache.hadoop.mapreduce.jobhistory.JobFinished']['finishTime'])
        except:
            finishTime = (
                p['event']['org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletion']['finishTime'])
        elapsedTime = (finishTime - startTime) / 1000
        listTime.append(elapsedTime)

with open('out.csv', 'wb') as outfile:
    writer = csv.writer(outfile, delimiter=',')
    with open('job_file_Scaled.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',')
        for index, row in enumerate(spamreader):
            if index == 0:
                continue
            # Controls the number of jobs you want to test in the trace
            if index == 100:
                break
            writer.writerow(row + [listTime[index - 1]])
