# File and Folder Structure

- different folders for different pipelines- one dockerfile per folder
- all terraform in the terraform folder- individual files for different concerns, or in main.tf if a one-liner. If sub-folders are required, a symlink can be made to link variables.tf and AWS provider