# Hyperscrape Coordinator

- The coordinator checks what files are present and not
    - The coordinator tells workers what files to download and what parts of them to download
    - The coordinator tells workers what reciever to upload files to
- Recievers recieves files from workers and uploads them to the large storage
    - The coordinator matches high-upload recievers to larger files
    - The reciever performs chunk hash comparisons
- Workers check what jobs are available
    - They download the chunks
    - They upload chunks to recievers