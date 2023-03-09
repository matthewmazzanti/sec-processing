package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"
)

type SecData struct {
	Filings struct {
		Recent SecFilings `json:"recent"`
		Files []SecFile `json:"files"`
	} `json:"filings"`
}

type SecFile struct {
	Name string `json:"name"`
	FilingCount int `json:"filingCount"`
	FilingFrom string `json:"filingFrom"`
	FilingTo string `json:"filingTo"`
}

type SecFilings struct {
	AccessionNumber []string `json:"accessionNumber"`
	FilingDate []string `json:"filingDate"`
	ReportDate []string `json:"reportDate"`
	AcceptanceDateTime []string `json:"acceptanceDateTime"`
	Act []string `json:"act"`
	Form []string `json:"form"`
	FileNumber []string `json:"fileNumber"`
	Items []string `json:"items"`
	Size []int `json:"size"`
	IsXBRL []int `json:"isXBRL"`
	IsInlineXBRL []int `json:"isInlineXBRL"`
	PrimaryDocument []string `json:"primaryDocument"`
	PrimaryDocDescription []string `json:"primaryDocDescription"`
}

type SecFiling struct {
	AccessionNumber string `json:"accessionNumber"`
	FilingDate string `json:"filingDate"`
	ReportDate string `json:"reportDate"`
	AcceptanceDateTime string `json:"acceptanceDateTime"`
	Act string `json:"act"`
	Form string `json:"form"`
	FileNumber string `json:"fileNumber"`
	Items string `json:"items"`
	Size int `json:"size"`
	IsXBRL int `json:"isXBRL"`
	IsInlineXBRL int `json:"isInlineXBRL"`
	PrimaryDocument string `json:"primaryDocument"`
	PrimaryDocDescription string `json:"primaryDocDescription"`
}

func (f *SecFilings) Transpose() []SecFiling {
	size := len(f.AccessionNumber)
	res := make([]SecFiling, size)

	for i := 0; i < size; i++ {
		filing := res[i]
		filing.AccessionNumber = f.AccessionNumber[i]
		filing.FilingDate = f.FilingDate[i]
		filing.ReportDate = f.ReportDate[i]
		filing.AcceptanceDateTime = f.AcceptanceDateTime[i]
		filing.Act = f.Act[i]
		filing.Form = f.Form[i]
		filing.Items = f.Items[i]
		filing.Size = f.Size[i]
		filing.IsXBRL = f.IsXBRL[i]
		filing.IsInlineXBRL = f.IsInlineXBRL[i]
		filing.PrimaryDocument = f.PrimaryDocument[i]
		filing.PrimaryDocDescription = f.PrimaryDocDescription[i]
	}

	return res
}

// Result of the calculation. Since this is going to be returned from a thread,
// multiple results may come back out of order, so we need to ensure that the
// return value, the date, is paired with the input parameter, the filename
type FilingResult struct {
	filename string
	date time.Time
}

var recentRe = regexp.MustCompile(`^CIK\d{10}.json$`)
var submissionRe = regexp.MustCompile(`^CIK\d{10}-submissions-\d{3}.json$`)

type ZipWorker struct {
	submissions map[string]*zip.File
}

func (zw *ZipWorker) Work(zipFile *zip.File) (string, error) {
	name := zipFile.FileHeader.Name

	file, err := zipFile.Open()
	if err != nil {
		return "", err
	}

	var data SecData
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return "", err
	}

	err = file.Close()
	if err != nil {
		return "", err
	}

	for _, refSecFile := range data.Filings.Files {
		name := refSecFile.Name
		_, ok := zw.submissions[name]
		if !ok {
			return "", fmt.Errorf("filename not found %s", name)
		}
	}

	l := len(data.Filings.Recent.Transpose())
	
	return fmt.Sprintf("%s, %d", name, l), nil
}

func main() {
	f, err := zip.OpenReader("submissions.zip")
	if err != nil {
		log.Fatalln(err)
	}

	recents := make([]*zip.File, 0)
	submissions := make(map[string]*zip.File, 0)
	for i := 0; i < len(f.File); i++ {
		file := f.File[i]
		name := file.FileHeader.Name

		if recentRe.MatchString(name) {
			recents = append(recents, file)
		}

		if submissionRe.MatchString(name) {
			submissions[name] = file
		}

		// There's also a placeholder.txt file, not useful
	}

	worker := ZipWorker{ submissions: submissions }


	buffer := 10
	files := make(chan Tagged[string, *zip.File], buffer)
	outChan, errChan := NewWorkerPool(files, worker.Work, 10, buffer)

	go func() {
		for _, recent := range recents {
			files <- tag(recent.FileHeader.Name, recent)
		}
		close(files)
	}()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		for tag := range outChan {
			fmt.Printf("%s\n", tag.Value)
		}
		wg.Done()
	}()
	go func() {
		for tag := range errChan {
			fmt.Printf("Error: %s, %s\n", tag.Tag, tag.Value)
		}
		wg.Done()
	}()
	wg.Wait()
}
