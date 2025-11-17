# File and Folder Structure

In-Progress: notes:
- keep files and folders modular: contents should not reach out of scope of its primary concerns
    - example: terraform folder should host all terraform contents. ETL folder for pipeline x should host all contents for that pipeline, with ETL being split into separate files (or folders if pushed by different dockerfiles)
    - example: terraform: separate files/folders for different resources, with a symlink to variable and aws setup files in main terraform folder
