import pandas as pd

# Number of rows to sample and chunk size
sample_size = 1_000_000  # Desired sample size
chunk_size = sample_size // 10     # Set chunk size to something reasonable based on memory

# Define the file paths
input_file = "data/the-reddit-covid-dataset-comments.csv"
output_file = f"data/subset/{sample_size}-reddit-covid-comments.csv"

# Initialize variables to track rows sampled
rows_sampled = 0

# Open the output file in write mode to initialize it
with open(output_file, "w") as f:
    # Read the CSV in chunks and sample from each
    for i, chunk in enumerate(pd.read_csv(input_file, chunksize=chunk_size)):
        # Calculate how many rows are left to sample
        rows_left_to_sample = sample_size - rows_sampled
        
        # If we've already sampled enough rows, break the loop
        if rows_left_to_sample <= 0:
            break
        
        # Sample from the chunk only what's needed
        sampled_chunk = chunk.sample(n=min(len(chunk), rows_left_to_sample), random_state=42)
        
        # Write the sampled chunk to the output file
        sampled_chunk.to_csv(f, index=False, header=f.tell() == 0)  # Write header only for the first chunk
        
        # Update the total number of sampled rows
        rows_sampled += len(sampled_chunk)
        
        print(f"Processed chunk {i+1}, total sampled rows: {rows_sampled}")

print(f"Sampled {rows_sampled} rows and saved to {output_file}")
