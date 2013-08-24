package main

import (
	"bufio"
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	machine string	= "winlab1"
	port string			= "6080"
	admin string 		= "admin"
	pwd string 			= "admin"
	
	gpMaxInstances float64 = 5
	publishers int = 7
)

var (
	client http.Client
	token  string
)

func init() {
	client = http.Client{}
}

/*
 * Checks the response received from ArcGIS Server.
 */
func parseJSON(resp *http.Response) map[string]interface{} {
	resBytes, _ := ioutil.ReadAll(resp.Body)

	// Decode into JSON
	var respJson interface{}
	if string(resBytes) == "" {
		str := "{}"
		json.Unmarshal([]byte(str), &respJson)
		return map[string]interface{}{"status": "error"}
	} else if err := json.Unmarshal(resBytes, &respJson); err != nil {
		fmt.Println(resp.Status)
		fmt.Println(resp.StatusCode)
		fmt.Println("JSON Unmarshaling error ", string(resBytes))
	}

	m := respJson.(map[string]interface{})
	if resp.StatusCode != 200 { // server error
		log.Print("Server error ")
	}
	return m
}

/*
 * Makes a POST request and returns a response.
 * Caller must close the response body
 */
func makeRequest(verb, reqUrl string, args, headers map[string]string) (*http.Response, error) {

	// Request object
	var req *http.Request

	// Add default params
	if args == nil {
		args = make(map[string]string)
	}
	_, fExists := args["f"]
	if !fExists {
		args["f"] = "json"
	}
	_, tokenExists := args["token"]
	if !tokenExists {
		args["token"] = token
	}

	switch verb {
	case "POST":
		{
			// Encode params
			var values = url.Values{}
			if args != nil {
				for k, v := range args {
					values.Set(k, v)
				}
			}
			req, _ = http.NewRequest(verb, reqUrl, strings.NewReader(values.Encode()))

			// Attach standard headers
			req.Header.Add("Content-type", "application/x-www-form-urlencoded")
		}
	case "GET":
		{
			// Append arguments to the URL
			if args != nil && len(args) > 0 {
				reqUrl += "?"
				for k, v := range args {
					reqUrl += k + "=" + v + "&"
				}
			}
			reqUrl = strings.TrimRight(reqUrl, "&")

			req, _ = http.NewRequest(verb, reqUrl, nil)
		}
	}

	// Additional request Headers
	if headers != nil {
		for k, v := range headers {
			req.Header.Add(k, v)
		}
	}

	// Send request
	resp, err := client.Do(req)
	return resp, err
}

func uploadRequest(reqUrl string, args, headers map[string]string, filepath string) (*http.Response, error) {
	// Create new writer
	buf := new(bytes.Buffer)
	w := multipart.NewWriter(buf)

	// token type field
	var tokenWriter io.Writer
	var err error
	if tokenWriter, err = w.CreateFormField("token"); err != nil {
		log.Fatal(err)
	}
	tokenWriter.Write([]byte(token))

	// Response type field
	if tokenWriter, err = w.CreateFormField("f"); err != nil {
		log.Fatal(err)
	}
	tokenWriter.Write([]byte("json"))

	// Create file field
	fd, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	fw, err := w.CreateFormFile("itemFile", fd.Name())
	if err != nil {
		log.Fatal(err)
	}

	// Write file field from file to upload
	_, err = io.Copy(fw, fd)
	if err != nil {
		log.Fatal(err)
	}
	// Important if you do not close the multipart writer you will not have a
	// terminating boundry
	w.Close()

	var req *http.Request
	if req, err = http.NewRequest("POST", reqUrl, buf); err != nil {
		log.Fatal(err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	res, err := client.Do(req)
	return res, err
}

/*
 * Obtains a token
 */
func getToken(username, password, serverName, port string) string {
	tokenURL := "http://" + serverName + ":" + port +
		"/arcgis/admin/generateToken"

	params := map[string]string{
		"username": username,
		"password": password,
		"client":   "requestip"}

	resp, err := makeRequest("POST", tokenURL, params, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	// Parse JSON
	m := parseJSON(resp)
	return m["token"].(string)
}

type ServiceInfo struct {
	sd, folder, service, cluster string
}

func (s *ServiceInfo) SetSD(sd string) {
	s.sd = sd
}
func (s *ServiceInfo) SetFolder(folder string) {
	s.folder = folder
}
func (s *ServiceInfo) SetService(service string) {
	s.service = service
}
func (s *ServiceInfo) SetCluster(cluster string) {
	s.cluster = cluster
}

func NewServiceInfo(sd string) ServiceInfo {
	return ServiceInfo{sd, "", strings.TrimSuffix(sd,".sd"), "default"}
}

func deleteAllUploadItems() {
	// All uploads
	machine := "winlab1"
	port := "6080"
	contextUrl := "http://" + machine + ":" + port + "/arcgis/admin"
	var resp *http.Response
	var err error
	if resp, err = makeRequest("GET", contextUrl+"/uploads", nil, nil); err != nil {
		log.Fatal(err)
	}
	allItems := parseJSON(resp)["items"].([]interface{})
	resp.Body.Close()

	done := make(chan int, len(allItems))

	for _, v := range allItems {
		value := v.(map[string]interface{})
		itemID := value["itemID"].(string)

		//go func(id string) {
		func(id string) {
			deleteUrl := contextUrl + "/uploads/" + itemID + "/delete"
			fmt.Println("Deleting item ", itemID, "at ", deleteUrl)
			if resp, err = makeRequest("POST", deleteUrl, nil, nil); err != nil {
				log.Fatal(err)
			}			

			responseJson := parseJSON(resp)
			if responseJson["status"] != "success" {
				fmt.Println("Failed to delete item " + itemID)
			}
			
			resp.Body.Close()
			done <- 1
		}(itemID)
	}

	for i := 0; i < len(allItems); i++ {
		<-done
		//fmt.Println("Done item ", (i + 1))
	}
}

func main() {
	t0 := time.Now()

	// Context URL
	contextUrl := "http://" + machine + ":" + port + "/arcgis/admin"
	servicesUrl := "http://" + machine + ":" + port + "/arcgis/rest/services"

	// Admin token
	token = getToken(admin, pwd, machine, port)

	// Delete all previously uploaded items
	deleteAllUploadItems()

	// Some globals
	var f *os.File
	var err error
	var resp *http.Response

	pubList := list.New()

	// Open input file
	if f, err = os.Open("c:/github/go-projects/src/publish/services.txt"); err != nil {
		log.Fatal(err)				
	}

	// Read each line (skipping lines starting with #)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var info ServiceInfo
		tokens := strings.Split(line, "|")
		for _, token := range tokens {
			k, v := func(parts []string) (string, string) {
				return parts[0], parts[1]
			}(strings.Split(token, "="))
			if k == "SD" {
				info = NewServiceInfo(v)			
				info.SetSD(v)
			} else if k == "folderName" {
				info.SetFolder(v)
			} else if k == "serviceName" {
				info.SetService(v)
			} else if k == "clusterName" {
				info.SetCluster(v)
			}
		}
		// Add to list
		pubList.PushBack(info)
	}

	fmt.Println("# services = ", pubList.Len())

	// get a list of machines in the site
	machinesUrl := contextUrl + "/machines"
	if resp, err = makeRequest("GET", machinesUrl, nil, nil); err != nil {
		log.Fatal(err)
	}	
	machinesJson := parseJSON(resp)["machines"].([]interface{})
	resp.Body.Close()
	machines := make([]string, len(machinesJson))
	for i, v := range machinesJson {
		values := v.(map[string]interface{})
		machines[i] = values["adminURL"].(string)
	}

	// For blocking until all done
	doneChan := make(chan map[string]string, pubList.Len())

	// standard service types
	serviceTypes := []string{"MapServer", "GeometryServer", "GPServer", "ImageServer", 
	 	"GeocodeServer","GeodataServer", "FeatureServer", "GlobeServer", "SearchServer"}

	// To track deletes
	deleteDone := make(chan int, pubList.Len())
	for e := pubList.Front(); e != nil; e = e.Next() {
		// delete service if it exists
		go func(info ServiceInfo) {
			defer func() {
				deleteDone <- 1
			}()
			service := strings.Split(info.sd, ".")[0]
			if !strings.EqualFold(info.service, "") {
				service = info.service
			}

			for _, value := range serviceTypes {
				// Check if the service exists
				existsUrl := contextUrl + "/services/" + info.folder + "/" + service + "." + value
				if resp, err = makeRequest("GET", existsUrl, nil, nil); err != nil {
					log.Println(err)
				}
				respJson := parseJSON(resp)
				resp.Body.Close()
				if respJson["code"] != nil {
					if respJson["code"].(float64) == 404 {
						continue
					}
				}

				// Delete it
				adminUrl := machines[rand.Intn(len(machines))] // randomly pick a machine
				deleteUrl := adminUrl + "/services/" + info.folder + "/" + service + "." + value + "/delete"
				fmt.Println(deleteUrl)
				if resp, err = makeRequest("POST", deleteUrl, nil, nil); err != nil {
					log.Println(err)
				}				
				respJson = parseJSON(resp)
				if respJson["status"] != nil {
					if respJson["status"].(string) != "success" {
						log.Println("Failed to delete service ", parseJSON(resp)["messages"])
					}
				}
				resp.Body.Close()
			}
		}(e.Value.(ServiceInfo))
	}

	for j := 0; j < pubList.Len(); j++ {
		<-deleteDone
	}
	fmt.Println("Old services deleted")

	// Edit publishing tool max instances
	pubUrl := contextUrl + "/services/System/PublishingTools.GPServer"
	if resp, err = makeRequest("GET", pubUrl, nil, nil); err != nil {
		log.Fatal(err)
	}	
	jsonResp := parseJSON(resp)
	resp.Body.Close()
	minCnt := jsonResp["minInstancesPerNode"].(float64)

	// Increase it to 5
	if minCnt != gpMaxInstances {
		jsonResp["minInstancesPerNode"] = gpMaxInstances
		jsonResp["maxInstancesPerNode"] = gpMaxInstances
		editUrl := contextUrl + "/services/System/PublishingTools.GPServer/edit"
		jsonBytes, _ := json.Marshal(jsonResp)
		resp, err = makeRequest("POST", editUrl,
			map[string]string{"service": string(jsonBytes)}, nil)
		if err != nil {
			log.Fatal(err)
		}
		jsonResp = parseJSON(resp)
		resp.Body.Close()
		if jsonResp["status"] == "error" {
			fmt.Println("Failed to increase min instance count increased to 5 ", jsonResp["messages"])
			os.Exit(0)
		} else {
			fmt.Println("Min instance count increased to 5 ", jsonResp["status"])
		}
	}

	// Allow only 5 simultanous publishers
	semaphoresChan := make(chan int, publishers)
	for i := 1; i <= publishers; i++ {
		semaphoresChan <- 1
	}

	// For each item
	for e := pubList.Front(); e != nil; e = e.Next() {
		info := e.Value.(ServiceInfo)

		// Upload & Publish
		go func(srvInfo ServiceInfo) {		
			// Grab Semaphore			
			<-semaphoresChan
			
			// Eventually release
			defer func () {				
				semaphoresChan <- 1
				fmt.Println("Released semaphore")
			}()

			// Upload
			fmt.Println("Uploading ", info.sd)
			adminUrl := machines[rand.Intn(len(machines))] // randomly pick one from the available machines
			resp, err = uploadRequest(adminUrl+"/uploads/upload", nil, nil, info.sd)
			if err != nil {
				log.Fatal(err)
			}			
			itemJson := parseJSON(resp)
			resp.Body.Close()

			if itemJson["status"].(string) == "error" {
				log.Println(itemJson["messages"])
				doneChan <- map[string]string{info.sd:"failed"}
				return
			}

			// Get itemId
			item := itemJson["item"].(map[string]interface{})
			itemID := item["itemID"].(string)

			// Get service configuration json
			if resp, err = makeRequest("GET", adminUrl+"/uploads/"+itemID+"/serviceconfiguration.json",
				nil, nil); err != nil {
				log.Println(err)
			}			
			srvConfigJson := parseJSON(resp) // returns map[string]interface{}
			resp.Body.Close()

			// Edit service configuration if there are overrides
			if srvInfo.cluster != "" || srvInfo.folder != "" || srvInfo.service != "" {
				service := srvConfigJson["service"].(map[string]interface{})
				if srvInfo.folder != "" {
					srvConfigJson["folderName"] = srvInfo.folder
				}
				if srvInfo.cluster != "" {
					service["clusterName"] = srvInfo.cluster
				}
				if srvInfo.service != "" {
					service["serviceName"] = srvInfo.service
				}
			}

			// Publish job
			configBytes, err := json.Marshal(srvConfigJson)
			if err != nil {
				log.Println(err)
			}
			if resp, err = makeRequest("POST", servicesUrl+"/System/PublishingTools/GPServer/Publish%20Service%20Definition/submitJob",
				map[string]string{
					"in_sdp_id":           itemID,
					"in_config_overwrite": string(configBytes)}, nil); err != nil {
			}			
			jobIdResp := parseJSON(resp)
			resp.Body.Close()

			// Got the jobid
			jobID := jobIdResp["jobId"].(string)

			// Sleep before getting status
			time.Sleep(5 * time.Second)

			// Poll for job status
			status := "esriJobSubmitted"
			var jobStatus map[string]interface{}
			for status == "esriJobSubmitted" || status == "esriJobExecuting" {
				// Get job status
				if resp, err = makeRequest("GET",
					servicesUrl+"/System/PublishingTools/GPServer/Publish%20Service%20Definition/jobs/"+jobID+"/status", nil, nil); err != nil {
				}
				jobStatus = parseJSON(resp)
				status = jobStatus["jobStatus"].(string)
				//fmt.Println(status)

				// Sleep a little
				time.Sleep(10 * time.Second)				
				resp.Body.Close()
			}

			if status != "esriJobSucceeded" {
				doneChan <- map[string]string{srvInfo.service:"failed"}
			} else {
				doneChan <- map[string]string{srvInfo.service:"succeeded"}
			}
			
			// Delete the uploaded item (fire and forget)
			go func() {
				deleteUrl := adminUrl + "/uploads/" + itemID + "/delete"
				resp, err = makeRequest("POST", deleteUrl, nil, nil)
				resp.Body.Close()
			}()
		}(info)
	}

	// Wait for all and collect results 
	results := map[string]string{}
	for i := 0; i < pubList.Len(); i++ {
		result := <- doneChan
		for k, v := range result {
			results[k] = v
		}
	}
	// Print summary
	for k, v := range results {
		fmt.Println(k,":",v)	
	}
	fmt.Println("Completed in ", time.Now().Sub(t0))
}
