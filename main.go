package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Operation string

const (
	SEARCH Operation = "search"
	PARSE  Operation = "translate"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func listBucket(client *s3.S3, bucket string, fromKey string, limit int64) []*s3.Object {
	var input *s3.ListObjectsInput
	if fromKey == "" {
		input = &s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			MaxKeys: aws.Int64(limit),
		}
	} else {
		input = &s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			MaxKeys: aws.Int64(limit),
			Marker:  aws.String(fromKey),
		}
	}

	result, err := client.ListObjects(input)

	if err != nil {
		exitErrorf("Could not list bucket ", bucket, err)
	}

	return result.Contents
}

func readObject(client *s3.S3, bucket string, key string) []byte {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := client.GetObject(input)
	if err != nil {
		exitErrorf("Could not read object ", key, err)
	}

	defer result.Body.Close()

	body, err := ioutil.ReadAll(result.Body)
	if err != nil {
		exitErrorf("Could not read bytes", err)
	}

	return body
}

func searchKeywords(client *s3.S3, bucket string, key string, keywords []string) (bool, []byte) {
	text := readObject(client, bucket, key)

	for _, kw := range keywords {
		if strings.Contains(string(text), kw) {
			return true, text
		}
	}

	return false, nil
}

func worker(client *s3.S3, bucket string, key string, keywords []string) {

	if status, text := searchKeywords(client, bucket, key, keywords); status {
		var parsed Log
		json.Unmarshal(text, &parsed)

		if parsed.Data.UserId == "" {
			parsed.Data.UserId = "null"
		}

		dirName := fmt.Sprintf("logs/%s", parsed.Data.UserId)
		if _, err := os.Stat(dirName); os.IsNotExist(err) {
			err := os.Mkdir(dirName, 07777)
			if err != nil {
				exitErrorf("Could not create directory", err)
			}
		}

		err := os.WriteFile(fmt.Sprintf("%s/%s.json", dirName, key), text, 0777)
		if err != nil {
			exitErrorf("Could not write to file", err)
		}
	}
}

func iterateBucket(client *s3.S3, bucket string, count *int, fromKey string, keywords []string) string {
	maxLimit := 1000
	lastKey := fromKey
	// fmt.Println("iterating from", lastKey, maxLimit, "current count", *count)
	fmt.Printf("[INFO] - page %d\n", (*count / maxLimit))

	objects := listBucket(client, bucket, lastKey, int64(maxLimit))

	for index, item := range objects {
		go worker(client, bucket, *item.Key, keywords)

		*count++
		if *item.Key == lastKey {
			return lastKey
		}
		if index == maxLimit-1 {
			return iterateBucket(client, bucket, count, *item.Key, keywords)
		}
	}

	return "null"
}

func readKeywords() []string {
	bytes, err := os.ReadFile("keywords.csv")
	if err != nil {
		exitErrorf("keywords.csv could not be read")
	}

	keywords := strings.Split(string(bytes), "\n")
	var result []string
	for _, kw := range keywords {
		if kw != "" {
			result = append(result, kw)
		}
	}
	return result
}

func searchLogs() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		exitErrorf(err.Error())
	}

	client := s3.New(sess)

	bucket := "******"

	count := 0
	foundLog := "" 
	keywords := readKeywords()

	fmt.Println("[INFO] - Searching for keywords:")
	for _, kw := range keywords {
		fmt.Println("\t", kw)
	}

	iterateBucket(client, bucket, &count, foundLog, keywords)
}

func translateLogs() {
	source := "logs"
	target := "events"
	out := "out"

	sourceEntries, err := os.ReadDir(source)
	if err != nil {
		exitErrorf("Could not read source directory", err)
	}

	for _, entry := range sourceEntries {
		userLogs, err := os.ReadDir(path.Join(source, entry.Name()))
		if err != nil {
			exitErrorf("Could not read user directory", err)
		}

		for _, userLog := range userLogs {
			bytes, err := os.ReadFile(path.Join(source, entry.Name(), userLog.Name()))
			if err != nil {
				exitErrorf("Could not read user log", err)
			}

			var log Log
			json.Unmarshal(bytes, &log)

			// create one directory for each user
			dirName := path.Join(target, log.Data.UserId)
			if _, err := os.Stat(dirName); os.IsNotExist(err) {
				err := os.Mkdir(dirName, 07777)
				if err != nil {
					exitErrorf("Could not create directory", err)
				}
			}

			// create one file for each log
			f, err := os.OpenFile(path.Join(dirName, userLog.Name()+".txt"), os.O_WRONLY|os.O_CREATE, 0777)

			_, erroR := f.WriteString(log.ToString())
			if erroR != nil {
				exitErrorf("Could not write to file", erroR)
			}

			csvFile, _ := os.OpenFile(path.Join(out, log.Data.UserId+".csv"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 07777)
			_, error := csvFile.WriteString(log.ToString())
			if error != nil {
				exitErrorf("Cloud not write to csv file", error)
			}
		}
	}
}

func main() {
	args := os.Args
	if len(args) == 1 || (len(args) > 1 && args[1] == string(PARSE)) {
		translateLogs()
	} else {
		searchLogs()
	}

}
