from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
import re

# Initialize Spark Context and Spark Session
sc = SparkContext("local", "Reddit Comments Processing")
spark = SparkSession(sc)

# Step 1: Load Data
# Load the raw text file
raw_rdd = sc.textFile("data/scrambled/scrambled-reddit-covid-comments-new-v2.txt")


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
        body = " ".join(body_lines).strip()
        body = clean_comment_body(body)
        sub_reddit, post_id, comment_id = extract_link_info(link)
        yield (link, int(created_utc), int(score), sub_reddit, post_id, comment_id, body)

comments_rdd = raw_rdd \
    .mapPartitions(lambda partition: map_to_comment_blocks(partition)) \
    .filter(lambda x: all(x)) 


comments_df = comments_rdd.toDF(["link", "created_utc", "score", "sub_reddit", "post_id", "comment_id", "body",])

# Step 5: Write DataFrame to CSV (you can specify the output path)
output_path = "data/final-test/reddit-covid-parquet-full.parquet"
comments_df.write.parquet(output_path)

