
import re
from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

input_path = "hdfs://namenode:9000/data/reddit-input.txt"
raw_rdd = sc.textFile(input_path, minPartitions=6)


def extract_link_info(link):
    pattern = r"https://old\.reddit\.com/r/([^/]+)/comments/([^/]+)/[^/]+/([^/]+)/?"
    match = re.match(pattern, link)
    if match:
        sub_reddit = match.group(1)
        post_id = match.group(2)
        comment_id = match.group(3)
        return sub_reddit, post_id, comment_id
    return None, None, None

def clean_comment_body(body):
    body = body.lower()
    body = re.sub(r'&[a-z]+;', '', body)
    body = re.sub(r'http\S+|www.\S+', '', body)
    body = re.sub(r'[^a-zA-Z0-9\s.!?]', '', body)

    body = ' '.join(body.split())

    return body

def map_to_comment_blocks(lines):
    link = ""
    created_utc = ""
    score = ""
    body_lines = []
    for line in lines:
        line = line.strip()
        if line.startswith("link:"):
            if link and created_utc and score and body_lines:
                body = " ".join(body_lines).strip()
                body = clean_comment_body(body)
                sub_reddit, post_id, comment_id = extract_link_info(link)
                yield (link, int(created_utc), int(score), sub_reddit, post_id, comment_id, body)
            link = line.replace("link:", "").strip()
            created_utc = ""
            body_lines = []
        elif line.startswith("created_utc:"):
            created_utc = line.replace("created_utc:", "").strip()
        elif line.startswith("score:"):
            score = line.replace("score:", "").strip()
        elif line.startswith("body:"):
            body_lines.append(line.replace("body:", "").strip())
        else:
            body_lines.append(line)

    
    if link and created_utc and score and body_lines:
        # Return none because possible unfinished body
        return


comments_rdd = raw_rdd \
    .mapPartitions(lambda partition: map_to_comment_blocks(partition)) \
    .filter(lambda x: all(x))


comments_df = comments_rdd.toDF(["link", "date", "score", "sub_reddit", "post_id", "comment_id", "body"])

output_path = "hdfs://namenode:9000/data/cleaned/reddit-comments-full-v3.parquet"
comments_df.write.parquet(output_path)

spark.stop()