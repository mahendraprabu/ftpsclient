
# Implementation of a concurrent FTPS downloader in golang
This is completed implementation of the concurrent FTP downloader program mentioned in following blog post. Please check this Blog post for the simple version of this program: "Designing a concurrent FTP downloader in Go" https://hackr.in/designing-a-concurrent-ftp-downloader-in-golang/

Pre-requisite: This program uses the following goftp client. Please download it first with go get command.

 go get github.com/secsy/goftp

## config.properties file

config.properties is a JSON file used by the FTPS downloader program to get FTPS server, login credential and other details given below.

LocalTempPath - is a temporary folder where files can be saved while file download is in progress

LocalPath - is destination folder where files are saved after download. A downstream program can collect files from this folder and process.

BackupPath - A backup or archive folder to keep all files downloaded

FtpPath - FTP folder

FileNameFilter - e.g., .txt, .dat etc

ConnectRetry - number of connection retry attempt if FTPS server not reachable

RetryInterval - Wait time in minutes between connection retry attempts

### Sample config.properties file entries:

{  

   "Title":"Concurrent FTPS Downloader",
   
   "User":"mahendra",
   
   "Password":"Secret123",
   
   "Server":"127.0.0.1:21",
   
   **"Protocol":"FTPS",**
   
   **"TLSMode":"Explicit",**
   
   "LocalTempPath":"C:/download/temp_download/",
   
   "LocalPath":"C:/download/Destination/",
   
   "BackupPath":"C:/download/backup/",
   
   "FtpPath":"/test/",
   
   "FileNameFilter":"",
   
   "ConnectRetry":3,
   
   "RetryInterval":3,
   
   "LogFile": "logs/FTPS_Client_log"
   
}
