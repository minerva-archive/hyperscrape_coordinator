# For Workers
## POST `/register`
Registers a worker with the coordinator

### Request
```json
{
    "max_upload": 100,
    "max_download": 100
}
```
- `max_upload` - max upload speed in mbps
- `max_download` - max download speed in mbps

### Response
```json
{
    "token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff"
}
```

## GET `/get_jobs?n=10`
Gets jobs for the worker to execute concurrently
### Request
- `n` - the number of jobs to get

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register`

### Response
```json
[
    {
        "job_id": "9b38d89a-f7b5-4676-af33-d2c54f16fa50",
        "url": "https://myrient.erista.me/files/No-Intro/Ouya%20-%20Ouya/iMech%20Online%20%28World%29%20%28v1.2.05%29.zip",
        "range": [0, 242422],
        "destination": "https://uploader.hackerdude.tech/upload"
    }
]
```
- Array of `job` objects
    - `job_id` a uuid to represent the job
    - `url` is the target URL to download
    - `range` is the target section of data to download
    - `destination` is the URL to the uploaded to upload files to (see `hyperscrape_reciever`)

## PUT `/status`
### Request
```json
{
    "current_progress":
    {
        "9b38d89a-f7b5-4676-af33-d2c54f16fa50": {
            "downloaded": 100345,
            "uploaded": 0
        }
    }
}
```
- `current_progress` is a JSON array with the current jobs and their progress, the numbers are measured in bytes

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register`

### Response
No content, returns status code `200`

# For Recievers
## POST `/register_reciever`
Registers a reciever with the coordinator

### Request
```json
{
    "max_upload": 100,
    "reciever_token": "c363b4fe-2c75-44a7-a3df-28064ef99630"
}
```
- `max_upload` - max upload speed in mbps
- `reciever_token` - Secret token used to ensure recievers are authorized

### Response
```json
{
    "token": "5e4bb24a-ea14-4ec8-8956-dfc551a9c2ff"
}
```

## GET `/file_info?job=JOB_ID`
Gets a file info from a job id

### Request Headers
- `authorization` - Set to `Bearer TOKEN` where `TOKEN` is the recieved response from `/register`

### Request
- `JOB_ID` - The ID of the job to get file info for

### Response
```json
{
    "file_path": "/No-Intro/wjhatever",
    "total_size": 34323
}
```
- `file_path` the path for the file to be uploaded
- `total_size` the total file size