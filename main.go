package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SecData struct {
	Filings struct {
		Recent Filing `json:"recent"`
	} `json:"filings"`
}

type Filing struct {
	FilingDates []string `json:"filingDate"`
}

func mostRecentFiling(filename string) (*time.Time, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Go's JSON decoder. Can read from file objects with the NewDecoder and
	// strings with Unmarshal.
	//
	// While reading, go reflects the struct to read the tags - the items
	// marked `json:"pathName"` above - to read data into the struct. This
	// gives you a nice way to build types for the data you are accessing.
	//
	// Note the use of the & to pass a pointer. This function doesn't return
	// data, just fills the passed pointer.
	//
	// TODO: Reading all the data into memory may be less-than-ideal.
	// Streaming json via the Token() call in the decoder may use less
	// memory and improve cache locality - at the cost of complexity
	var data SecData
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return nil, err
	}

	// Read into new variable for convinence
	filingDates := data.Filings.Recent.FilingDates

	// Early exit in case there are no filing dates
	if len(filingDates) == 0 {
		return nil, nil
	}

	
	// The date here is go's format string. Yes this is terrible.
	date, err := time.Parse("2006-01-02", filingDates[0])
	if err != nil {
		return nil, err
	}

	// Range over the rest of the filing dates to see if any are newer than
	// the first
	//
	// Couple things here:
	// - Range always returns a pair of the index and value from the slice.
	// - Slice notation creates a new "view" of the underlying array, so
	// unlike python this requires no new allocations
	//
	// This loop may not be needed, the dates seem to be in order. Makes for
	// a good example, and a trivial fix either way
	for _, filingDate := range filingDates[1:] {
		nextDate, err := time.Parse("2006-05-04", filingDate)
		if err != nil {
			return nil, err
		}

		if nextDate.After(date) {
			date = nextDate
		}
	}

	return &date, nil
}

// Result of the calculation. Since this is going to be returned from a thread,
// multiple results may come back out of order, so we need to ensure that the
// return value, the date, is paired with the input parameter, the filename
type FilingResult struct {
	filename string
	date time.Time
}

// A manager struct for the channels and waitgroup. A channel is a FIFO queue
// for typed values in go. It allows data producers to send data to it, and will
// block block until that data is recieved. The wait group is a bounded
// semaphore, used to track the number of jobs running
//
// Create a struct to manage the threading state. We have three values we care
// about from the context of the runner:
// - The input channel - filenames. This is where the rest of the program will
// send values to the runner to be processed.
// - The output channel - results. This is where the runner will send output
// results.
// - A "Wait group" - a bounded semaphore. Channels need to be closed from
// alternative threads, so this provides a way for us to know that all
// processing has completed
type Runner struct {
	filenames <-chan string
	results chan<- FilingResult
	wg *sync.WaitGroup
}

// Create a new runner. This returns the runner to interact with, and the input
// and output channels to send data to
func NewRunner(filenames <-chan string, count int) (Runner, <-chan FilingResult) {
	results := make(chan FilingResult)
	runner := Runner{
		filenames: filenames,
		results: results,
		wg: &sync.WaitGroup{},
	}
	runner.runCount(count)

	return runner, results
}

// Start up n jobs to run the work
func (r *Runner) runCount(n int) {
	r.wg.Add(n)
	for i := 0; i < n; i++ {
		go r.run()
	}

	// Once all of the jobs have finished processing, close the result
	// channel. For this example, this is not needed, but for longer
	// runnning processes this is proper behavior
	go func() {
		r.wg.Wait()
		close(r.results)
	}()
}

func (r *Runner) run() {
	// Recieve filename from input channel
	for filename := range r.filenames {
		// Run the check
		date, err := mostRecentFiling(filename)

		// Want to continue processing if there are errors. Log a
		// message and continue
		if err != nil {
			fmt.Printf("Error for file %s, %v\n", filename, err)
			continue
		}

		if date == nil {
			fmt.Printf("No dates in file %s\n", filename)
			continue
		}

		// Send result over output channel
		r.results <- FilingResult{
			filename: filename,
			date: *date,
		}
	}

	// Notify that the work has completed. This executes once the filenames
	// channel is closed and the loop exits
	r.wg.Done()
}

func main() {
	filenames := make(chan string)
	_, results := NewRunner(filenames, 5)

	go func() {
		// Ignore error, only errors on malformed glob
		// TODO: This may not be the correct approach for almost a
		// million files. filepath.Walk may prove a better abstraction
		globFiles, _ := filepath.Glob("data/*.json")
		for _, filename := range globFiles {
			filenames <- filename
		}
		close(filenames)
	}()

	for res := range results {
		fmt.Printf("file: %s, date: %v\n", res.filename, res.date)
	}
}
