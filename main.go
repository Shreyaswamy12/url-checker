package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Result struct {
	URL        string
	Success    bool
	StatusCode int
	ErrorType  string
}

func worker(id int, jobs <-chan string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for url := range jobs {
		fmt.Println("Worker", id, "checking URL:", url)
		result := checkURL(url)
		results <- result

		if result.Success {
			fmt.Println(result.URL, "is reachable. Status Code:", result.StatusCode)
		} else {
			fmt.Println(result.URL, "failed. Status Code:", result.StatusCode, "Error:", result.ErrorType)
		}
	}

}

func checkURL(url string) Result {

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, Success: false, StatusCode: 0, ErrorType: "request"}
	}
	//send request

	response, err := http.DefaultClient.Do(request)

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return Result{URL: url, Success: false, StatusCode: 0, ErrorType: "timeout"}
		}
		return Result{URL: url, Success: false, StatusCode: 0, ErrorType: "network"}
	}

	defer response.Body.Close()

	if response.StatusCode >= 200 && response.StatusCode < 400 {
		return Result{URL: url, Success: true, StatusCode: response.StatusCode, ErrorType: ""}
	}

	errorType := "http_other"

	if response.StatusCode >= 400 && response.StatusCode < 500 {
		errorType = "http_4xx"
	} else if response.StatusCode >= 500 {
		errorType = "http_5xx"
	}

	return Result{URL: url, Success: false, StatusCode: response.StatusCode, ErrorType: errorType}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("please provide file path and worker count as command line arguments")
		return
	}

	// Get the file path from command line arguments

	filePath := os.Args[1]

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}

	// Ensure the file is closed when the function exits
	defer file.Close()

	//create a scanner
	scanner := bufio.NewScanner(file) //connects to reader

	// create a slice to hold the URLs
	var urls []string

	for scanner.Scan() {
		url := strings.TrimSpace(scanner.Text())
		if url != "" {
			urls = append(urls, url)
		}
		// checkURL(url) // Check the URL for reachability

	}

	//errors
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	// start workers
	// workerCount := 3 //hardcoded
	workerCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Please provide a valid worker count")
		return
	}
	if workerCount <= 0 {
		fmt.Println("Worker count must be greater than zero")
		return
	}
	results := make(chan Result, len(urls))

	jobs := make(chan string) // create a channel to send jobs to workers
	var wg sync.WaitGroup     // create a wait group to wait for all workers to finish

	startTime := time.Now()

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}
	for _, url := range urls {
		jobs <- url
	}
	close(jobs)
	wg.Wait()

	close(results)

	var successful int
	var failed int
	errorCounts := make(map[string]int)

	for result := range results {
		if result.Success {
			successful++
		} else {
			failed++
			errorCounts[result.ErrorType]++
		}
	}

	executionTime := time.Since(startTime)

	fmt.Println("\n========== FINAL SUMMARY ==========")
	fmt.Println("Total URLs checked:", len(urls))
	fmt.Println("Successful:", successful)
	fmt.Println("Failed:", failed)

	fmt.Println("\nError breakdown:")
	for errorType, count := range errorCounts {
		fmt.Println(errorType+":", count)
	}

	fmt.Println("Total execution time:", executionTime)
	fmt.Println("===================================")

	fmt.Println("all URLs checked")
	// // Print the URLs
	// for _, url := range urls {
	// 	fmt.Println(url)
	// }
	// fmt.Println("Total URLs read:", len(urls))

}
