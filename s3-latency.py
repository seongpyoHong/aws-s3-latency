import os
import boto3
import time
from boto3.s3.transfer import S3Transfer

# S3 Client 생성
s3 = boto3.client('s3')
response = s3.list_buckets()

#Create File (1KB, 10KB, 1MB, 10MB)
small_file_min = 'C:\\Users\\ghdtj\\Desktop\\HW#6-1\\1KB'
small_file_max = 'C:\\Users\\ghdtj\\Desktop\\HW#6-1\\10KB'
Large_file_min = 'C:\\Users\\ghdtj\\Desktop\\HW#6-1\\1MB'
Large_file_max = 'C:\\Users\\ghdtj\\Desktop\\HW#6-1\\10MB'

# Load bucket list
buckets = [bucket['Name'] for bucket in response['Buckets']]

#Upload (PUT)
for bucket in buckets:
    SumOfTime_1KB = 0
    SumOfTime_10KB = 0
    SumOfTime_1MB = 0
    SumOfTime_10MB = 0
    location = bucket.split('-')[3]
    print("%s"% location)
    # 10KB PUT
    for i in range(1,11):
        StartTime_10KB = time.time()
        s3.upload_file(small_file_max, bucket, '10KB_%d' % i)
        FinshedTime_10KB = time.time()
        SumOfTime_10KB += (FinshedTime_10KB - StartTime_10KB)

    # 1KB PUT
    for i in range(1, 11):
        StartTime_1KB = time.time()
        s3.upload_file(small_file_min, bucket, '1KB_%d'%i)
        FinshedTime_1KB = time.time()
        SumOfTime_1KB += (FinshedTime_1KB - StartTime_1KB)

    # 1MB PUT
    for i in range(1, 11):
        StartTime_1MB = time.time()
        s3.upload_file(Large_file_min, bucket, '1MB_%d'%i)
        FinshedTime_1MB = time.time()
        SumOfTime_1MB += (FinshedTime_1MB - StartTime_1MB)

    # 10MB PUT
    for i in range(1, 11):
        StartTime_10MB = time.time()
        s3.upload_file(Large_file_max, bucket, '10MB_%d'%i)
        FinshedTime_10MB = time.time()
        SumOfTime_10MB += (FinshedTime_10MB - StartTime_10MB)

    print("Average Upload Time 1KB in %s : " % location, SumOfTime_1KB / 10)
    print("Average Upload Time 10KB in %s : "% location, SumOfTime_10KB / 10)
    print("Average Upload Time 1MB in %s : " % location, SumOfTime_1MB / 10)
    print("Average Upload Time 10MB in %s : "% location, SumOfTime_10MB / 10)

#Download (GET)
for bucket in buckets:
    SumOfTime_1KB = 0
    SumOfTime_10KB = 0
    SumOfTime_1MB = 0
    SumOfTime_10MB = 0
    location = bucket.split('-')[3]
    print("%s"% location)
    # 10KB GET
    for i in range(1,11):
        StartTime_10KB = time.time()
        s3.download_file(bucket, '10KB_%d'%i, '10KB_%s_%d' % (location,i))
        FinshedTime_10KB = time.time()
        SumOfTime_10KB += (FinshedTime_10KB - StartTime_10KB)

    # 1KB GET
    for i in range(1, 11):
        StartTime_1KB = time.time()
        s3.download_file(bucket, '1KB_%d' % i, '1KB_%s_%d' % (location, i))
        FinshedTime_1KB = time.time()
        SumOfTime_1KB += (FinshedTime_1KB - StartTime_1KB)

    # 1MB GET
    for i in range(1, 11):
        StartTime_1MB = time.time()
        s3.download_file(bucket, '1MB_%d' % i, '1MB_%s_%d' % (location, i))
        FinshedTime_1MB = time.time()
        SumOfTime_1MB += (FinshedTime_1MB - StartTime_1MB)

    # 10MB GET
    for i in range(1, 11):
        StartTime_10MB = time.time()
        s3.download_file(bucket, '10MB_%d' % i, '10MB_%s_%d' % (location, i))
        FinshedTime_10MB = time.time()
        SumOfTime_10MB += (FinshedTime_10MB - StartTime_10MB)

    print("Average Download Time 1KB in %s : " % location, SumOfTime_1KB / 10)
    print("Average Download Time 10KB in %s : "% location, SumOfTime_10KB / 10)
    print("Average Download Time 1MB in %s : " % location, SumOfTime_1MB / 10)
    print("Average Download Time 10MB in %s : "% location, SumOfTime_10MB / 10)

#DELETE
re=boto3.resource('s3')
for bucket in buckets:
    SumOfTime_1KB = 0
    SumOfTime_10KB = 0
    SumOfTime_1MB = 0
    SumOfTime_10MB = 0
    location = bucket.split('-')[3]
    print("%s"% location)
    # 10KB DELETE
    for i in range(1,11):
        StartTime_10KB = time.time()
        re.Object(bucket, '10KB_%d'%i).delete()
        FinshedTime_10KB = time.time()
        SumOfTime_10KB += (FinshedTime_10KB - StartTime_10KB)
    # 1KB DELETE
    for i in range(1, 11):
        StartTime_1KB = time.time()
        re.Object(bucket, '1KB_%d'%i).delete()
        FinshedTime_1KB = time.time()
        SumOfTime_1KB += (FinshedTime_1KB - StartTime_1KB)
    # 1MB DELETE
    for i in range(1, 11):
        StartTime_1MB = time.time()
        re.Object(bucket, '1MB_%d'%i).delete()
        FinshedTime_1MB = time.time()
        SumOfTime_1MB += (FinshedTime_1MB - StartTime_1MB)
    # 10MB DELETE
    for i in range(1, 11):
        StartTime_10MB = time.time()
        re.Object(bucket, '10MB_%d'%i).delete()
        FinshedTime_10MB = time.time()
        SumOfTime_10MB += (FinshedTime_10MB - StartTime_10MB)
    print("Average Delete Time 1KB in %s : " % location, SumOfTime_1KB / 10)
    print("Average Delete Time 10KB in %s : "% location, SumOfTime_10KB / 10)
    print("Average Delete Time 1MB in %s : " % location, SumOfTime_1MB / 10)
    print("Average Delete Time 10MB in %s : "% location, SumOfTime_10MB / 10)
