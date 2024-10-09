import kagglehub

# Download latest version
path = kagglehub.dataset_download("pavellexyr/the-reddit-covid-dataset")

print("Path to dataset files:", path)