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

var (
	machine, port, admin, pwd, inputfile string
	
	gpMaxInstances float64 = 5
	threads int = 5
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
		return map[string]interface{}{"status": "error"}
	}

	m := respJson.(map[string]interface{})
	if resp.StatusCode != 200 { // server error
		log.Print("Server error ")
	}
	return m
}

func makeRequestForJSON(verb, reqUrl string, args, headers map[string]string) map[string]interface{} {
	var httpResp *http.Response
	var err error
	if httpResp, err = makeRequest(verb, reqUrl, args, headers); err != nil {
		log.Fatal(err)
	}
	responseJson := parseJSON(httpResp)	
	httpResp.Body.Close()
	
	return responseJson
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

	return client.Do(req)
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

	tokenJson := makeRequestForJSON("POST", tokenURL, params, nil)
	return tokenJson["token"].(string)
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
	contextUrl := "http://" + machine + ":" + port + "/arcgis/admin"
	
	allItemsJson := makeRequestForJSON("GET", contextUrl+"/uploads", nil, nil)
	allItems := allItemsJson["items"].([]interface{})

	done := make(chan int, len(allItems))

	for _, v := range allItems {
		value := v.(map[string]interface{})
		itemID := value["itemID"].(string)

		//go func(id string) {
		func(id string) {
			deleteUrl := contextUrl + "/uploads/" + itemID + "/delete"
			fmt.Println("Deleting item ", itemID, "at ", deleteUrl)
			
			responseJson := makeRequestForJSON("POST", deleteUrl, nil, nil)
			if responseJson["status"] != "success" {
				fmt.Println("Failed to delete item " + itemID)
			}
			
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
	fmt.Println("This tool publishes services described in a pipe-deliminted text file.")
	fmt.Println("For examples, refer http://bit.ly/19JbADR\n")
	
	fmt.Printf("Enter input as space separated values: [server] [port] [user] [password] [Path to pipe-delimited text file]\n") 
	fmt.Fscanf(os.Stdin, "%s %s %s %s %s", &machine, &port, &admin, &pwd, &inputfile)

	//TODO: Check if server is running
	//TODO: Check if input file exists
		
	// Context URL
	contextUrl := "http://" + machine + ":" + port + "/arcgis/admin"
	servicesUrl := "http://" + machine + ":" + port + "/arcgis/rest/services"

	// Admin token
	token = getToken(admin, pwd, machine, port)

	// Delete all uploads
	deleteAllUploadItems()

	// Some globals
	pubList := list.New()

	// Open input file
	var f *os.File
	var err error
	if f, err = os.Open(inputfile); err != nil {
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
	
	machinesJson := makeRequestForJSON("GET", machinesUrl, nil, nil)["machines"].([]interface{})

	machines := make([]string, len(machinesJson))
	for i, v := range machinesJson {
		values := v.(map[string]interface{})
		machines[i] = values["adminURL"].(string)
	}

	// For blocking until all done
	doneChan := make(chan map[string]string, pubList.Len())

	// Allow only 5 simultanous publishers
	semaphoresChan := make(chan int, threads)
	for i := 1; i <= threads; i++ {
		semaphoresChan <- 1
	}

	// standard service types
	serviceTypes := []string{"MapServer", "GeometryServer", "GPServer", "ImageServer", 
	 	"GeocodeServer","GeodataServer", "FeatureServer", "GlobeServer", "SearchServer"}

	// To track deletes
	fmt.Println("Deleting old services")
	deleteDone := make(chan int, pubList.Len())
	for e := pubList.Front(); e != nil; e = e.Next() {
		// delete service if it exists
		go func(info ServiceInfo) {
			// Grab Semaphore
			<-semaphoresChan
			
			// Eventually release
			defer func () {
				semaphoresChan <- 1
			}()
			
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
				respJson := makeRequestForJSON("GET", existsUrl, nil, nil)
				if respJson["status"] != nil && respJson["status"] == "error" {
					continue
				} else if respJson["code"] != nil {
						if respJson["code"].(float64) == 404 {
						continue
					}
				}

				// Delete it
				adminUrl := machines[rand.Intn(len(machines))] // randomly pick a machine
				deleteUrl := adminUrl + "/services/" + info.folder + "/" + service + "." + value + "/delete"
				fmt.Println("Deleting ", deleteUrl)
				
				respJson = makeRequestForJSON("POST", deleteUrl, nil, nil)
				if respJson["status"] != nil {
					if respJson["status"].(string) != "success" {
						log.Println("Failed to delete service ", respJson)
					}
				}
			}
		}(e.Value.(ServiceInfo))
	}

	for j := 0; j < pubList.Len(); j++ {
		<-deleteDone
	}
	fmt.Println("Old services deleted")

	// Edit publishing tool max instances
	fmt.Println("Increasing number of instances of the Publishing tool")
	pubUrl := contextUrl + "/services/System/PublishingTools.GPServer"	
	jsonResp := makeRequestForJSON("GET", pubUrl, nil, nil)
	minCnt := jsonResp["minInstancesPerNode"].(float64)

	// Increase it to 5
	if minCnt != gpMaxInstances {
		jsonResp["minInstancesPerNode"] = gpMaxInstances
		jsonResp["maxInstancesPerNode"] = gpMaxInstances
		editUrl := contextUrl + "/services/System/PublishingTools.GPServer/edit"
		jsonBytes, _ := json.Marshal(jsonResp)

		jsonResp := makeRequestForJSON("POST", editUrl, map[string]string{"service": string(jsonBytes)}, nil)
		if jsonResp["status"] == "error" {
			fmt.Println("Failed to increase min instance count increased to 5 ", jsonResp["messages"])
			os.Exit(0)
		} else {
			fmt.Println("Min instance count increased to 5 ", jsonResp["status"])
		}
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
			}()

			// Upload
			adminUrl := machines[rand.Intn(len(machines))] // randomly pick one from the available machines
			var httpResp *http.Response
			var err error
			httpResp, err = uploadRequest(adminUrl+"/uploads/upload", nil, nil, info.sd) 
			if err != nil {
				log.Fatal(err)
			}			
			itemJson := parseJSON(httpResp)
			httpResp.Body.Close()

			if itemJson["status"].(string) == "error" {
				log.Println(itemJson["messages"])
				doneChan <- map[string]string{info.sd:"failed"}
				return
			}

			// Get itemId
			item := itemJson["item"].(map[string]interface{})
			itemID := item["itemID"].(string)

			// Get service configuration json
			srvConfigJson := makeRequestForJSON("GET", adminUrl+"/uploads/"+itemID+"/serviceconfiguration.json", nil, nil)

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

//			// Grab Semaphore
//			<-semaphoresChan
//			
//			// Eventually release
//			defer func () {
//				semaphoresChan <- 1
//			}()

			// Publish job
			configBytes, err := json.Marshal(srvConfigJson)
			if err != nil {
				log.Println(err)
			}
			
			jobIdResp := makeRequestForJSON("POST", 
					servicesUrl+"/System/PublishingTools/GPServer/Publish%20Service%20Definition/submitJob", 
					map[string]string{"in_sdp_id":itemID,"in_config_overwrite": string(configBytes)}, nil)

			// Got the jobid
			jobID := jobIdResp["jobId"].(string)

			// Sleep before getting status
			time.Sleep(5 * time.Second)

			// Poll for job status
			status := "esriJobSubmitted"
			for status == "esriJobSubmitted" || status == "esriJobExecuting" {
				// Get job status
				jobStatus := makeRequestForJSON("GET", 
					servicesUrl+"/System/PublishingTools/GPServer/Publish%20Service%20Definition/jobs/"+jobID+"/status", 
					nil, nil)

				if jobStatus["jobStatus"] != nil {
					status = jobStatus["jobStatus"].(string)
				}
				
				// Sleep a little
				time.Sleep(5 * time.Second)				
			}

			if status != "esriJobSucceeded" {
				doneChan <- map[string]string{srvInfo.service:"failed"}
			} else {
				doneChan <- map[string]string{srvInfo.service:"succeeded"}
			}
			
			// Delete the uploaded item (fire and forget)
			go func() {
				makeRequestForJSON("POST", adminUrl+"/uploads/"+itemID+"/delete", nil, nil)
			}()
		}(info)
	}

	// Wait for all and collect results 
	results := map[string]string{}
	for i := 0; i < pubList.Len(); i++ {
		result := <- doneChan
		for k, v := range result {
			results[k] = v
			fmt.Println(k ,":",v)
		}
	}
	// Print summary
	for k, v := range results {
		fmt.Println(k,":",v)	
	}
	fmt.Println("Completed in ", time.Now().Sub(t0))
}