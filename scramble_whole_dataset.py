import pandas as pd

path = 'data/the-reddit-covid-dataset-comments.csv' 
# subset_path = 'data/subset/100000-reddit-covid-comments.csv'
output_path = 'data/scrambled/scrambled-reddit-covid-comments-full-v2.txt' 

# Define the chunk size
chunk_size = 10000  # Adjust this based on memory capacity

# Function to process each chunk
def process_chunk(chunk, outfile):
    for index, row in chunk.iterrows():
        # Extract the relevant fields
        permalink = row['permalink']
        created_utc = row['created_utc']
        body = row['body']
        score = row['score']
        # type = row['type']
        # id = row['id']
        # subreddit_id = row['subreddit.id']
        # subreddit_name = row['subreddit.name']
        # subreddit_nsfw = row['subreddit.nsfw']
        # Write them in the desired format
        outfile.write(f"link:{permalink}\n")
        outfile.write(f"created_utc:{created_utc}\n")
        outfile.write(f"score:{score}\n")
        # outfile.write(f"type:{type}\n")
        # outfile.write(f"id:{id}\n")
        # outfile.write(f"subreddit_id:{subreddit_id}\n")
        # outfile.write(f"subreddit_name:{subreddit_name}\n")
        # outfile.write(f"subreddit_nsfw:{subreddit_nsfw}\n")
        outfile.write(f"body:{body}\n")

# Read the CSV in chunks and process each chunk
with open(output_path, 'w', encoding='utf-8') as outfile:
    for chunk in pd.read_csv(path, chunksize=chunk_size, encoding='utf-8'):
        process_chunk(chunk, outfile)
