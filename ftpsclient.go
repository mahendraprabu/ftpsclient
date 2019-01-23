package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/secsy/goftp"
)

func main() {
	//Load FTPS connection details from config file
	initConfig := LoadConfig("config.properties")
	fmt.Println("FTPS Download -", initConfig.Title)
	fmt.Println(initConfig)

	//Endless for loop to repeat download
	//for {
	StartFtpDownload(initConfig)
	//}
}

//StartFtpDownload - initiate the FTP download
func StartFtpDownload(initConfig InitConfig) error {
	//maxConnection is maximum number of FTPS download tasks to run in parallel
	maxConnection := 3
	maxFiles := 50
	startTime := time.Now()
	logFileName := initConfig.LogFile + "_" + startTime.Format("20060102") + ".txt"
	var f *os.File
	f, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//Report error and exit program
		log.Printf("ERROR: Could not create or Open log file:%s\r\n", logFileName)
		return err
	}
	log.SetOutput(f)
	defer f.Close()

	//Connect to FTPS server
	var conn *goftp.Client
	conn, err = FtpsConnect(initConfig)
	if err != nil {
		log.Printf("ERROR: FTPS Connection Error. Exiting StartFtpDownload..\r\n")
		return err
	}
	defer conn.Close()
	log.Printf("Successfully Connected to FTPS server.\r\n")

	var wg sync.WaitGroup
	fileChan := make(chan os.FileInfo, maxFiles)

	wg.Add(1)
	go FtpList(fileChan, &wg, conn, initConfig)
	wg.Add(maxConnection)

	//Launch multiple FTP download clients (go routines)
	for i := 1; i <= maxConnection; i++ {
		go FtpDownloadClient(fileChan, &wg, i, conn, initConfig)
	}

	//Wait for all FTPList and FtpDownloadClient goroutines to complete
	wg.Wait()
	log.Printf("File Download complete\r\n")

	//wait for 10 minutes before next download
	totalTime := time.Since(startTime)
	if totalTime < 3*time.Minute {
		//Add some logic here to wait for few minutes or hours before repeat the download process
		//time.Sleep(10 * time.Minute)
	}
	return nil
}

//FtpList lists all files in FTP folder and pass the file names to FTP clients via File channel
func FtpList(fileChan chan os.FileInfo, wg *sync.WaitGroup, conn *goftp.Client, initConfig InitConfig) error {
	defer wg.Done()
	defer close(fileChan) //Close the channel so that all ftpclients can exit in case of any network errors
	maxFileCount := 100

	//List files in FTP folder
	fileList, err := conn.ReadDir(initConfig.FtpPath)
	if err != nil {
		//Return to main if files can not be listed
		log.Printf("ERROR: Could not read FTP directory. %v\r\n", err)
		return err
	}

	//Print files list
	var fileCount int
	var filesToDownload []os.FileInfo

	log.Printf("Listing Remote FTP Files\r\n")

	for _, file := range fileList {
		if !file.IsDir() {
			//Dont download if file name does not match with file name filter
			if initConfig.FileNameFilter != "" {
				if !strings.HasSuffix(file.Name(), initConfig.FileNameFilter) {
					continue
				}
			}
			fileCount++
			filesToDownload = append(filesToDownload, file)
			log.Printf("%d\t%s\t%v\t%v\t%v\r\n", fileCount, file.Name(), file.ModTime(), file.Size(), file.Mode())
		}
	}
	log.Printf("Total files to download: %d\r\n", fileCount)

	for i, fileName := range filesToDownload {
		if i > maxFileCount {
			break
		}
		fileChan <- fileName
	}
	return nil
}

//FtpDownloadClient will download files in parallel
func FtpDownloadClient(fileChan chan os.FileInfo, wg *sync.WaitGroup, clientNum int, conn *goftp.Client, initConfig InitConfig) error {
	defer wg.Done()

	//log.Println("Client -", clientNum, " - Starts Downloading files")
	var downloadCount int

	for file := range fileChan {
		remoteFile := initConfig.FtpPath + file.Name()
		localFile, err := os.Create(initConfig.LocalTempPath + file.Name())
		if err != nil {
			log.Printf("ERROR: Could not create local file. Skipping file:%s\r\n", file.Name())
			return err
		}
		startTime := time.Now()
		log.Printf("Start download:%v\r\n", remoteFile)
		err2 := conn.Retrieve(remoteFile, localFile)
		if err2 != nil {
			log.Printf("ERROR: Could not download file %v from FTPS server.\r\n", file.Name())
			log.Printf("%v\r\n", err2)
			return err2
		}
		localFile.Close()
		totalTime := time.Since(startTime)
		log.Printf("SUCCESS: File: %v downloaded, Time taken: %v\r\n", file.Name(), totalTime)

		if err := validateFile(initConfig.LocalTempPath+file.Name(), file.Size()); err != nil {
			log.Printf("ERROR: File size mismatch. Downloaded File may be corrupted.\r\n")
			return err
		}

		err = conn.Delete(initConfig.FtpPath + file.Name())
		if err != nil {
			log.Printf("ERROR: Could not delete file from FTP folder, File name:%v\r\n", file.Name())
		}

		//go command will run backupAndMove in a seperate goroutine (or thread)
		wg.Add(1)
		go backupAndMove(initConfig.LocalTempPath+file.Name(), initConfig.LocalPath+file.Name(), initConfig.BackupPath+file.Name(), wg)

		downloadCount++
	}
	log.Println("Total number of files dowloaded by Client# ", clientNum, " is : ", downloadCount)
	return nil
}

func backupAndMove(tempFile, destFile, backupFile string, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)
	copyToDest(tempFile, destFile, wg)
	wg.Add(1)
	go moveToDest(tempFile, backupFile, wg)
}

//validate FTP file size with downloaded file size
func validateFile(file string, size int64) error {
	s, err := os.Open(file)
	if err != nil {
		return err
	}
	defer s.Close()
	if lfile, err3 := s.Stat(); err3 != nil {
		log.Printf("ERROR: %s - Could not get File info\r\n", file)
		return err3
	} else if lfile.Size() != size {
		log.Printf("ERROR: %s - File size mismatch. Downloaded File may be corrupted.\r\n", file)
		return errors.New("CorruptFile")
	}
	return nil
}

//FtpsConnect connect to FTPS server and return connection
//Added retry logic incase of connection error
func FtpsConnect(initConfig InitConfig) (*goftp.Client, error) {
	var config goftp.Config
	var sessionkey [32]byte
	copy(sessionkey[:], "MyFTPsessionkey")

	if initConfig.Protocol == "FTPS" {
		config = goftp.Config{
			User:     initConfig.User,
			Password: initConfig.Password,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
				SessionTicketKey:   sessionkey,
				ClientSessionCache: tls.NewLRUClientSessionCache(1),
			},
			TLSMode: goftp.TLSExplicit,
		}
	} else {
		config = goftp.Config{
			User:     initConfig.User,
			Password: initConfig.Password,
		}
	}

	//Connect to FTPS server
	conn, err := goftp.DialConfig(config, initConfig.Server)

	//Retry FTP connection in case of Connection error
	ftpRetry := 1
	var ftpError bool
	if err != nil {
		ftpError = true
		for ftpRetry < initConfig.ConnectRetry {
			ftpRetry++
			log.Printf("Waiting for a while before retry...\r\n")
			time.Sleep(time.Duration(initConfig.RetryInterval) * time.Minute)
			var err2 error
			ftpError = false
			conn, err2 = goftp.DialConfig(config, initConfig.Server)
			if err2 != nil {
				ftpError = true
				err = err2
			} else {
				ftpError = false
				break
			}
			//To-DO: Email Notification after 5 retries
		}
	}
	if ftpError {
		log.Printf("ERROR: FTPS Connection Error %v\r\n", err)
		return conn, err
	}
	return conn, nil
}

//copyToDest copies the src file to dst and delete local file
func copyToDest(src string, dst string, wg *sync.WaitGroup) error {
	//Decrement wg count when this function complete
	defer wg.Done()

	time.Sleep(5 * time.Second)
	// Open the source file for reading
	s, err := os.Open(src)
	if err != nil {
		log.Printf("ERROR: copyToDest: Could not read file: %v\r\n", src)
		log.Printf("%v\r\n", err)
		return err
	}
	defer s.Close()

	// Open the destination file for writing
	d, err := os.Create(dst)
	if err != nil {
		log.Printf("ERROR: copyToDest: Could not create remote file: %v\r\n", dst)
		return err
	}

	// Copy the contents of the source file into the destination file
	if _, err := io.Copy(d, s); err != nil {
		log.Printf("ERROR: copyToDest: Error in copying file to %v\r\n", dst)
		d.Close()
		return err
	}

	// Return any errors that result from closing the destination file
	// Will return nil if no errors occurred
	if err := d.Close(); err != nil {
		log.Printf("ERROR: copyToDest: Error closing remote file: %v\r\n", dst)
	}
	if err := s.Close(); err != nil {
		log.Printf("ERROR: copyToDest: Error closing local file: %v\r\n", src)
	}

	log.Printf("copyToDest: File: %v copied to %v\r\n", src, dst)
	//return nil if success
	return nil
}

//moveToDest will use copyToDest to copy file and then delete the source file
func moveToDest(src string, dst string, wg *sync.WaitGroup) {
	defer wg.Done()
	wg.Add(1)
	err := copyToDest(src, dst, wg)
	if err != nil {
		log.Printf("ERROR: moveToDest: ERROR in copying file: %v\r\n", src)
	} else {
		time.Sleep(5 * time.Second)
		log.Printf("moveToDest: Deleting File: %v\r\n", src)
		os.Remove(src)
	}
}

//InitConfig holds data about configuration information
type InitConfig struct {
	Title          string
	User           string
	Password       string
	Server         string
	Protocol       string
	TLSMode        string
	LocalTempPath  string
	LocalPath      string
	BackupPath     string
	FtpPath        string
	FileNameFilter string
	ConnectRetry   int
	RetryInterval  int
	LogFile        string
}

//LoadConfig reads the config.properties file and loads to InitConfig
func LoadConfig(file string) InitConfig {
	var initCfg InitConfig
	configFile, err := os.Open(file)
	if err != nil {
		log.Printf("ERROR: Could not open config file %v\r\n", file)
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&initCfg)
	if err != nil {
		log.Printf("ERROR: JSON Parser Error: %v\r\n", file)
		log.Printf(err.Error())
	}
	fmt.Println(initCfg)
	return initCfg
}
