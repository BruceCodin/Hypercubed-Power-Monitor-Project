# File and Folder Structure

- different folders for different pipelines - one dockerfile and requirements.txt per folder
- all terraform in the terraform folder - each pipeline should have it's own terraform folder with its own config to avoid conflicts with other pipelines. If sub-folders are required, a symlink can be made to link variables.tf and AWS provider
- all sql files in the db_schema folder within the rds_pipeline folder
