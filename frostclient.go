package frostclient

//package frostclient

//package frostclient

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/metno/roadlabels/pkg/db"
	"golang.org/x/exp/maps"
)

var clientID = "e7413001-3139-4f82-8162-e2f1960ea7fb"

type StationHolderReq struct {
	Context          string    `json:"@context"`
	Type             string    `json:"@type"`
	APIVersion       string    `json:"apiVersion"`
	License          string    `json:"license"`
	CreatedAt        time.Time `json:"createdAt"`
	QueryTime        float64   `json:"queryTime"`
	CurrentItemCount int       `json:"currentItemCount"`
	ItemsPerPage     int       `json:"itemsPerPage"`
	Offset           int       `json:"offset"`
	TotalItemCount   int       `json:"totalItemCount"`
	CurrentLink      string    `json:"currentLink"`
	Stations         []struct {
		Type        string `json:"@type"`
		ID          string `json:"id"`
		Name        string `json:"name"`
		ShortName   string `json:"shortName"`
		Country     string `json:"country"`
		CountryCode string `json:"countryCode"`
		Geometry    struct {
			Type        string    `json:"@type"`
			Coordinates []float64 `json:"coordinates"`
			Nearest     bool      `json:"nearest"`
		} `json:"geometry"`
		Masl           int       `json:"masl"`
		ValidFrom      time.Time `json:"validFrom"`
		County         string    `json:"county"`
		CountyID       int       `json:"countyId"`
		Municipality   string    `json:"municipality"`
		MunicipalityID int       `json:"municipalityId"`
		StationHolders []string  `json:"stationHolders"`
		ExternalIds    []string  `json:"externalIds"`
		WigosID        string    `json:"wigosId"`
	} `json:"data"`
}

type Station struct {
	Context          string    `json:"@context"`
	Type             string    `json:"@type"`
	APIVersion       string    `json:"apiVersion"`
	License          string    `json:"license"`
	CreatedAt        time.Time `json:"createdAt"`
	QueryTime        float64   `json:"queryTime"`
	CurrentItemCount int       `json:"currentItemCount"`
	ItemsPerPage     int       `json:"itemsPerPage"`
	Offset           int       `json:"offset"`
	TotalItemCount   int       `json:"totalItemCount"`
	CurrentLink      time.Time `json:"currentLink"`
	Data             []struct {
		SourceID      string    `json:"sourceId"`
		ReferenceTime time.Time `json:"referenceTime"`
		Observations  []struct {
			ElementID           string  `json:"elementId"`
			Value               float64 `json:"value"`
			Unit                string  `json:"unit"`
			TimeOffset          string  `json:"timeOffset"`
			TimeResolution      string  `json:"timeResolution"`
			TimeSeriesID        int     `json:"timeSeriesId"`
			PerformanceCategory string  `json:"performanceCategory"`
			ExposureCategory    string  `json:"exposureCategory"`
			QualityCode         int     `json:"qualityCode"`
		} `json:"observations"`
	} `json:"data"`
}

type ObsType struct {
	Context          string    `json:"@context"`
	Type             string    `json:"@type"`
	APIVersion       string    `json:"apiVersion"`
	License          string    `json:"license"`
	CreatedAt        time.Time `json:"createdAt"`
	QueryTime        float64   `json:"queryTime"`
	CurrentItemCount int       `json:"currentItemCount"`
	ItemsPerPage     int       `json:"itemsPerPage"`
	Offset           int       `json:"offset"`
	TotalItemCount   int       `json:"totalItemCount"`
	CurrentLink      string    `json:"currentLink"`
	Data             []struct {
		SourceID            string    `json:"sourceId"`
		ValidFrom           time.Time `json:"validFrom"`
		TimeOffset          string    `json:"timeOffset"`
		TimeResolution      string    `json:"timeResolution"`
		TimeSeriesID        int       `json:"timeSeriesId"`
		ElementID           string    `json:"elementId"`
		Unit                string    `json:"unit"`
		PerformanceCategory string    `json:"performanceCategory"`
		ExposureCategory    string    `json:"exposureCategory"`
		Status              string    `json:"status"`
		URI                 string    `json:"uri"`
	} `json:"data"`
}

type ObsReq struct {
	Context          string    `json:"@context"`
	Type             string    `json:"@type"`
	APIVersion       string    `json:"apiVersion"`
	License          string    `json:"license"`
	CreatedAt        time.Time `json:"createdAt"`
	QueryTime        float64   `json:"queryTime"`
	CurrentItemCount int       `json:"currentItemCount"`
	ItemsPerPage     int       `json:"itemsPerPage"`
	Offset           int       `json:"offset"`
	TotalItemCount   int       `json:"totalItemCount"`
	CurrentLink      string    `json:"currentLink"`
	Data             []struct {
		SourceID      string    `json:"sourceId"`
		ReferenceTime time.Time `json:"referenceTime"`
		Observations  []struct {
			ElementID           string  `json:"elementId"`
			Value               float32 `json:"value"`
			Unit                string  `json:"unit"`
			TimeOffset          string  `json:"timeOffset"`
			TimeResolution      string  `json:"timeResolution"`
			TimeSeriesID        int     `json:"timeSeriesId"`
			PerformanceCategory string  `json:"performanceCategory"`
			ExposureCategory    string  `json:"exposureCategory"`
			QualityCode         int     `json:"qualityCode"`
		} `json:"observations"`
	} `json:"data"`
}

func obsTypeReq(frostID string) (ObsType, error) {
	url := fmt.Sprintf("https://frost.met.no/observations/availableTimeSeries/v0.jsonld?sources=%s&elements=road_water_film_thickness,road_ice_thickness,road_snow_thickness&timeresolutions=PT10M", frostID)
	sh := ObsType{}

	client := http.Client{
		Timeout: 20 * time.Second,
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return sh, fmt.Errorf("http.Get(%s) failed: %v", url, err)
	}
	req.SetBasicAuth(clientID, "")
	resp, err := client.Do(req)
	if err != nil {
		return sh, fmt.Errorf("http.Get(%s) failed: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 { // 404 => No observations
		return sh, nil
	}
	if resp.StatusCode != 200 { // 404 => No observations
		return sh, fmt.Errorf("http.Get(%s) Unexpected response code %d", url, resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&sh)
	if err != nil {
		return sh, err
	}

	return sh, nil

}

func stationHolderReq(url string) (StationHolderReq, error) {

	sh := StationHolderReq{}

	resp, err := httpReq(url)
	if err != nil {
		return sh, fmt.Errorf("http.Get(%s) failed: ", url)
	}

	if resp.StatusCode != 200 {
		return sh, fmt.Errorf("http.Get(%s) Unexpected response code %d", url, resp.StatusCode)
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&sh)
	if err != nil {
		return sh, err
	}

	return sh, nil

}

var snMap = make(map[string]string)

func GetStationsWithSensor() (map[string]db.Camera, error) {
	camMap := make(map[string]db.Camera)

	sourcesMap := make(map[string]db.Camera)
	cams, err := db.GetCams()
	if err != nil {
		return sourcesMap, fmt.Errorf("db.GetCams(): %v", err)
	}
	for c := 0; c < len(cams); c++ {
		idParts := strings.Split(cams[c].ForeignID, "_")
		stID := idParts[0]
		camMap[stID] = cams[c]
	}

	res, err := stationHolderReq("https://frost.met.no/sources/v0.jsonld?stationholder=STATENS+VEGVESEN")
	if err != nil {
		return sourcesMap, fmt.Errorf("stationHolderReq(): %v", err)
	}
	elements := ""

	for s := 0; s < len(res.Stations); s++ {
		ok := false
		extid := ""
		for f := 0; f < len(res.Stations[s].ExternalIds); f++ {
			extid = res.Stations[s].ExternalIds[f]
			_, ok = camMap[extid]
			if ok {
				break
			}
		}

		if !ok {
			continue
		}

		obstypes, err := obsTypeReq(res.Stations[s].ID)
		if err != nil {
			log.Printf("obsTypeReq: %v", err)
			continue
		}

		if len(obstypes.Data) != 3 {
			continue
		}
		if !hasElm(obstypes, "road_water_film_thickness") || !hasElm(obstypes, "road_snow_thickness") || !hasElm(obstypes, "road_ice_thickness") {
			continue
		}
		if strings.Contains(res.Stations[s].ID, ":") {
			sourcesMap[res.Stations[s].ID] = camMap[extid]
		}
		for ot := 0; ot < len(obstypes.Data); ot++ {

			obstype := obstypes.Data[ot]
			sourcesMap[obstype.SourceID] = camMap[extid]

			fmt.Printf("**%s ", obstype.ElementID)
			elements += obstype.ElementID + ","
		}
		fmt.Printf("\n")
		time.Sleep(time.Second * 2)
	}
	fmt.Printf("%+v\n\n", sourcesMap)

	return sourcesMap, nil
}

func hasElm(s ObsType, str string) bool {
	for _, v := range s.Data {
		if v.ElementID == str {
			return true
		}
	}

	return false
}

func getStationsWithIceSensor_Road_Ice_Thickness() {
	camMap := make(map[string]db.Camera)
	cams, err := db.GetCams()
	if err != nil {
		log.Printf("db.GetCams(): %v", err)
		os.Exit(1)
	}
	for c := 0; c < len(cams); c++ {
		idParts := strings.Split(cams[c].ForeignID, "_")
		stID := idParts[0]
		camMap[stID] = cams[c]
	}

	res, err := stationHolderReq("https://frost.met.no/sources/v0.jsonld?stationholder=STATENS+VEGVESEN")
	if err != nil {
		log.Printf("stationHolderReq(): %v", err)
		os.Exit(1)
	}

	for s := 0; s < len(res.Stations); s++ {
		for f := 0; f < len(res.Stations[s].ExternalIds); f++ {
			extid := res.Stations[s].ExternalIds[f]
			cam, ok := camMap[extid]
			if ok { // Station has camera

				obstypes, err := obsTypeReq(res.Stations[s].ID)
				if err != nil {
					log.Printf("obsTypeReq: %v", err)
					continue
				}
				for ot := 0; ot < len(obstypes.Data); ot++ {
					obstype := obstypes.Data[ot]
					fmt.Printf("%+v\n\n", obstype.ElementID)
					if obstype.ElementID == "road_ice_thickness" {

						//snMap[obstype.SourceID] = fmt.Sprintf("%d", cam.ID)
						fmt.Printf("\"%s\": %d,", obstype.SourceID, cam.ID)
					}
				}
				break
			}
		}
	}
	fmt.Printf("%+v\n\n", snMap)

}

func httpReq(url string) (*http.Response, error) {
	var (
		err      error
		response *http.Response
		retries  int = 10
	)

	client := http.Client{
		Timeout: 10 * time.Second,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("http.Get(%s) failed: %v", url, err)
	}
	req.SetBasicAuth(clientID, "")

	for retries > 0 {
		response, err = client.Do(req)
		if err != nil {
			log.Printf("Http request failed. %v. Retrying", err)
			retries -= 1
			time.Sleep(2 * time.Second)
		} else {
			if response.StatusCode != 200 {
				log.Printf("Http request failed with status code. %d. Retrying", response.StatusCode)
				retries -= 1
				time.Sleep(2 * time.Second)
			} else {
				break
			}
		}
	}
	return response, err
}

func obsRequest(sources string, elements string, timespan string) (ObsReq, error) {

	url := fmt.Sprintf("https://frost.met.no/observations/v0.jsonld?sources=%s&referencetime=%s&elements=%s&timeoffsets=PT0H&timeresolutions=PT10M&timeseriesids=0&performancecategories=C&exposurecategories=2", sources, timespan, elements)
	//fmt.Printf("RequesT: %s", url)
	_ = url

	sh := ObsReq{}

	resp, err := httpReq(url)
	if err != nil {
		return sh, fmt.Errorf("http.Get(%s) failed: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return sh, fmt.Errorf("http.Get(%s) Unexpected response code %d", url, resp.StatusCode)
	}

	err = json.NewDecoder(resp.Body).Decode(&sh)
	if err != nil {
		return sh, fmt.Errorf("obsRequest json.NewDecoder(resp.Body).Decode(&sh) failed: %v,", err)
	}

	return sh, nil
}

type IceObs struct {
	RefTime   time.Time
	Station   string
	ParamName string
	Value     float32
	CamID     int
}

type ObsRoadweather struct {
	RefTime            time.Time
	Station            string // SVV Station ID
	FrostID            string
	CamID              int // Internal camid sqlite db
	IceThickness       float32
	WaterFilmThickness float32
	SnowThickness      float32
	Class              int
}

const (
	DryE              int = 0
	WetE              int = 1
	SnowE             int = 2
	IceE              int = 3
	WetAndSnowE       int = 4
	WetAndIceE        int = 5
	SnowAndIceE       int = 6
	SnowAndIceAndWetE int = 7
)

const (
	Dry          int = 0
	Wet          int = 1 // No snow an Ice
	SnowAndOrIce int = 2
	//SnowAndOrIceWithWater    int = 2
	//SnowAndOrIceWithoutWater int = 3
)

const (
	ObsWater int = 0
	ObsSnow  int = 1
	ObsIce   int = 2
)

func GetObsMapForLabelApp() (map[string][]ObsRoadweather, error) {

	sourcesMap, err := GetStationsWithSensor()
	if err != nil {
		log.Fatalf("GetStationsWithSensor: %v", err)
	}

	keys := maps.Keys(sourcesMap)
	sources := strings.Join(keys, ",")
	fmt.Printf("Sources: %v\n", sourcesMap)

	// This is so long back we have image data
	start := time.Date(2023, 2, 10, 0, 0, 0, 00, time.UTC)
	//start := time.Date(2021, 2, 10, 0, 0, 0, 00, time.UTC)
	// No ice after .. may maybe?

	//stop := time.Date(2023, 5, 12, 0, 0, 0, 00, time.UTC)
	stop := time.Date(2023, 11, 16, 0, 0, 0, 00, time.UTC)
	//stop := time.Date(2023, 2, 12, 0, 0, 0, 00, time.UTC)

	from := start
	to := start.Add(24 * time.Hour)
	count := 0
	maxdays := stop.Sub(start).Hours() / 24
	log.Printf("Samples from %.0f days: ", maxdays)

	classesCount := map[string]int{
		"Dry":          0,
		"Wet":          0,
		"Snow":         0,
		"Ice":          0,
		"Wet+Snow":     0,
		"Wet+Ice":      0,
		"Snow+Ice":     0,
		"Snow+Ice+Wet": 0,
	}

	class2Obses := make(map[string][]ObsRoadweather)

	for from.Before(stop) {
		log.Printf("Getting obs batch %d of %0.0f .. ", count, maxdays)
		count++
		//2023-02-10T00:00:00Z
		timespan := fmt.Sprintf("%s/%s", from.Format("2006-01-02T15:04Z"), to.Format("2006-01-02T15:04Z"))
		//timespan := "2023-02-10T00:00Z/2023-02-11T00:00Z"
		resp, err := obsRequest(sources, "road_ice_thickness,road_water_film_thickness,road_snow_thickness", timespan)
		if err != nil {
			log.Printf("GetDataFromFrost obsRequest: httpresp: %v error: %v", resp, err)
			count--
			continue
		}

		times := resp.Data

		for t := 0; t < len(times); t++ {
			roadConditionClass := -1

			//if times[t].ReferenceTime.Hour() == 0 || times[t].ReferenceTime.Hour() == 6 || times[t].ReferenceTime.Hour() == 12 || times[t].ReferenceTime.Hour() == 18 { // forEach 6th hour

			if times[t].ReferenceTime.UTC().Minute() != 0 {
				continue
			}

			var iceThickness float32 = 0.0
			var waterThickness float32 = 0.0
			var snowThickness float32 = 0.0
			for o := 0; o < len(times[t].Observations); o++ {
				if times[t].Observations[o].Unit != "mm" { // Just in case ..
					log.Printf("GetFrostObses() Unsupported unit: %s", times[t].Observations[o].Unit)
					continue
				}
				if times[t].Observations[o].ElementID == "road_ice_thickness" {
					iceThickness = times[t].Observations[o].Value

				}
				if times[t].Observations[o].ElementID == "road_water_film_thickness" {
					waterThickness = times[t].Observations[o].Value
				}
				if times[t].Observations[o].ElementID == "road_snow_thickness" {
					snowThickness = times[t].Observations[o].Value
				}
			}

			if iceThickness == 0.0 && waterThickness == 0.0 && snowThickness == 0.0 {
				roadConditionClass = DryE

				classesCount["Dry"]++
			} else if iceThickness > 0.0 && waterThickness > 0.0 && snowThickness > 0.0 {
				roadConditionClass = SnowAndIceAndWetE
				classesCount["Snow+Ice+Wet"]++
			} else if iceThickness > 0.0 && snowThickness > 0.0 {
				roadConditionClass = SnowAndIceE
				classesCount["Snow+Ice"]++
			} else if iceThickness > 0.0 && waterThickness > 0.0 {
				roadConditionClass = WetAndIceE
				classesCount["Wet+Ice"]++
			} else if snowThickness > 0.0 && waterThickness > 0.0 {
				roadConditionClass = WetAndSnowE
				classesCount["Wet+Snow"]++
			} else if iceThickness > 0.0 { // => Ice only
				roadConditionClass = IceE
				classesCount["Ice"]++
			} else if snowThickness > 0.0 { // => Snow only
				roadConditionClass = SnowE
				classesCount["Snow"]++
			} else if waterThickness > 0.0 { // => Wateronly
				roadConditionClass = WetE
				classesCount["Wet"]++
			} else {
				panic("Logic error")
			}

			obs := ObsRoadweather{}
			obs.CamID = sourcesMap[times[t].SourceID].ID
			obs.Station = times[t].SourceID
			obs.IceThickness = iceThickness
			obs.WaterFilmThickness = waterThickness
			obs.SnowThickness = snowThickness
			obs.RefTime = times[t].ReferenceTime.UTC()
			obs.Class = roadConditionClass

			// Downsample huge majority class
			if roadConditionClass == Dry && (times[t].ReferenceTime.UTC().Hour() == 0 || times[t].ReferenceTime.UTC().Hour() == 12 ||
				times[t].ReferenceTime.UTC().Hour() == 6 || times[t].ReferenceTime.UTC().Hour() == 18) {
				class2Obses["Dry"] = append(class2Obses["Dry"], obs)
			} else if roadConditionClass == WetE {
				class2Obses["Water"] = append(class2Obses["Water"], obs)
			} else if roadConditionClass == SnowE {
				class2Obses["Snow"] = append(class2Obses["Snow"], obs)
			} else if roadConditionClass == IceE {
				class2Obses["Ice"] = append(class2Obses["Ice"], obs)
			} else if roadConditionClass == WetAndSnowE {
				class2Obses["Water+Snow"] = append(class2Obses["Water+Snow"], obs)
			} else if roadConditionClass == WetAndIceE {
				class2Obses["Water+Ice"] = append(class2Obses["Water+Ice"], obs)
			} else if roadConditionClass == SnowAndIceE {
				class2Obses["Snow+Ice"] = append(class2Obses["Snow+Ice"], obs)
			} else if roadConditionClass == SnowAndIceAndWetE {
				class2Obses["Snow+Ice+Water"] = append(class2Obses["Snow+Ice+Water"], obs)
			}
		}
		from = from.Add(24 * time.Hour)
		to = to.Add(24 * time.Hour)
	}

	log.Printf("\nClasscount: %+v", classesCount)

	return class2Obses, nil
}

func GetDataFromFrost8Classes() (map[int][]ObsRoadweather, error) {

	sourcesMap, err := GetStationsWithSensor()
	if err != nil {
		log.Fatalf("GetStationsWithSensor: %v", err)
	}

	keys := maps.Keys(sourcesMap)
	sources := strings.Join(keys, ",")
	fmt.Printf("Sources: %v\n", sourcesMap)

	// This is so long back we have image data
	start := time.Date(2023, 2, 10, 0, 0, 0, 00, time.UTC)
	stop := time.Now().UTC()
	from := start
	to := start.Add(24 * time.Hour)
	count := 0
	maxdays := stop.Sub(start).Hours() / 24
	log.Printf("Samples from %.0f days: ", maxdays)

	classesCount := map[string]int{
		"Dry":          0,
		"Wet":          0,
		"Snow":         0,
		"Ice":          0,
		"Wet+Snow":     0,
		"Wet+Ice":      0,
		"Snow+Ice":     0,
		"Snow+Ice+Wet": 0,
	}

	class2Obses := make(map[int][]ObsRoadweather)

	for from.Before(stop) {
		log.Printf("Getting obs batch %d of %0.0f .. ", count, maxdays)
		count++
		//2023-02-10T00:00:00Z
		timespan := fmt.Sprintf("%s/%s", from.Format("2006-01-02T15:04Z"), to.Format("2006-01-02T15:04Z"))
		//timespan := "2023-02-10T00:00Z/2023-02-11T00:00Z"
		resp, err := obsRequest(sources, "road_ice_thickness,road_water_film_thickness,road_snow_thickness", timespan)
		if err != nil {
			log.Printf("GetDataFromFrost obsRequest: httpresp: %v error: %v", resp, err)
			count--
			continue
		}

		times := resp.Data

		for t := 0; t < len(times); t++ {
			roadConditionClass := -1

			//if times[t].ReferenceTime.Hour() == 0 || times[t].ReferenceTime.Hour() == 6 || times[t].ReferenceTime.Hour() == 12 || times[t].ReferenceTime.Hour() == 18 { // forEach 6th hour

			if times[t].ReferenceTime.UTC().Minute() != 0 {
				continue
			}

			var iceThickness float32 = 0.0
			var waterThickness float32 = 0.0
			var snowThickness float32 = 0.0
			for o := 0; o < len(times[t].Observations); o++ {
				if times[t].Observations[o].Unit != "mm" { // Just in case ..
					log.Printf("GetFrostObses() Unsupported unit: %s", times[t].Observations[o].Unit)
					continue
				}
				if times[t].Observations[o].ElementID == "road_ice_thickness" {
					iceThickness = times[t].Observations[o].Value

				}
				if times[t].Observations[o].ElementID == "road_water_film_thickness" {
					waterThickness = times[t].Observations[o].Value
				}
				if times[t].Observations[o].ElementID == "road_snow_thickness" {
					snowThickness = times[t].Observations[o].Value
				}
			}

			if iceThickness == 0.0 && waterThickness == 0.0 && snowThickness == 0.0 {
				roadConditionClass = DryE
				classesCount["Dry"]++
			} else if iceThickness > 0.0 && waterThickness > 0.0 && snowThickness > 0.0 {
				roadConditionClass = SnowAndIceAndWetE
				classesCount["Snow+Ice+Wet"]++
			} else if iceThickness > 0.0 && snowThickness > 0.0 && waterThickness == 0.0 {
				roadConditionClass = SnowAndIceE
				classesCount["Snow+Ice"]++
			} else if iceThickness > 0.0 && waterThickness > 0.0 && snowThickness == 0.0 {
				roadConditionClass = WetAndIceE
				classesCount["Wet+Ice"]++
			} else if snowThickness > 0.0 && waterThickness > 0.0 && iceThickness == 0.0 {
				roadConditionClass = WetAndSnowE
				classesCount["Wet+Snow"]++
			} else if iceThickness > 0.0 { // => Ice only
				roadConditionClass = IceE
				classesCount["Ice"]++
			} else if snowThickness > 0.0 { // => Snow only
				roadConditionClass = SnowE
				classesCount["Snow"]++
			} else if waterThickness > 0.0 { // => Wateronly
				roadConditionClass = WetE
				classesCount["Wet"]++
			} else {
				panic("Logic error")
			}

			obs := ObsRoadweather{}
			obs.CamID = sourcesMap[times[t].SourceID].ID
			obs.Station = times[t].SourceID
			obs.IceThickness = iceThickness
			obs.WaterFilmThickness = waterThickness
			obs.SnowThickness = snowThickness
			obs.RefTime = times[t].ReferenceTime.UTC()
			obs.Class = roadConditionClass

			if times[t].ReferenceTime.UTC().Hour() == 0 || times[t].ReferenceTime.UTC().Hour() == 12 ||
				times[t].ReferenceTime.UTC().Hour() == 6 || times[t].ReferenceTime.UTC().Hour() == 18 {

				if roadConditionClass == Dry {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == WetE && (times[t].ReferenceTime.UTC().Hour() == 0 || times[t].ReferenceTime.UTC().Hour() == 12 ||
					times[t].ReferenceTime.UTC().Hour() == 6 || times[t].ReferenceTime.UTC().Hour() == 18) {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == SnowE {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == IceE {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == WetAndSnowE {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == WetAndIceE {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == SnowAndIceE {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				} else if roadConditionClass == SnowAndIceAndWetE {
					class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				}
			}
		}
		from = from.Add(24 * time.Hour)
		to = to.Add(24 * time.Hour)
	}

	log.Printf("\nClasscount: %+v", classesCount)

	return class2Obses, nil
}

// Dry  int = 0, Wet int = 1 // No snow an Ice, SnowAndOrIce int = 2
func GetDataFromFrost3Classes() (map[int][]ObsRoadweather, error) {

	sourcesMap, err := GetStationsWithSensor()
	if err != nil {
		log.Fatalf("GetStationsWithSensor: %v", err)
	}

	keys := maps.Keys(sourcesMap)
	sources := strings.Join(keys, ",")

	// This is so long back we have image data
	start := time.Date(2023, 2, 10, 0, 0, 0, 00, time.UTC)
	//start := time.Date(2021, 2, 10, 0, 0, 0, 00, time.UTC)
	// No ice after .. may maybe?

	//stop := time.Date(2023, 5, 12, 0, 0, 0, 00, time.UTC)
	//stop := time.Date(2023, 2, 12, 0, 0, 0, 00, time.UTC)
	//stop := time.Date(2023, 10, 12, 0, 0, 0, 00, time.UTC)
	stop := time.Now().UTC()
	from := start
	to := start.Add(24 * time.Hour)
	count := 0
	maxdays := stop.Sub(start).Hours() / 24
	log.Printf("Samples from %.0f days: ", maxdays)

	classesCount := map[string]int{
		"Dry":                            0,
		"Wet":                            0, // No snow or Ice
		"SnowAndOrIceWithOrWithoutWater": 0,
	}
	class2Obses := make(map[int][]ObsRoadweather)

	for from.Before(stop) {
		log.Printf("Getting obs batch %d of %0.0f .. ", count, maxdays)
		count++
		//2023-02-10T00:00:00Z
		timespan := fmt.Sprintf("%s/%s", from.Format("2006-01-02T15:04Z"), to.Format("2006-01-02T15:04Z"))
		//timespan := "2023-02-10T00:00Z/2023-02-11T00:00Z"
		resp, err := obsRequest(sources, "road_ice_thickness,road_water_film_thickness,road_snow_thickness", timespan)
		if err != nil {
			log.Printf("GetDataFromFrost obsRequest: httpresp: %v error: %v", resp, err)
			count--
			continue
		}

		times := resp.Data

		// Skip due to https://docs.google.com/spreadsheets/d/1xAF5QCVUiIHbwzYMpxVaqyiNqmhZ8ItdoNxlR6rMeqo/edit#gid=0
		// This is Frost IDs
		skipList := map[string]bool{
			"SN16620:0": true,
			"SN30244:0": true, // No snow or Ice
			"SN52390:0": true,
			"SN67153:0": true,
		}

		for t := 0; t < len(times); t++ {
			_, exist := skipList[times[t].SourceID]
			// If the key exists
			if exist {
				//fmt.Printf("%s in skiplist. Skipping\n", times[t].SourceID)
				continue
			}
			roadConditionClass := -1

			//if times[t].ReferenceTime.Hour() == 0 || times[t].ReferenceTime.Hour() == 6 || times[t].ReferenceTime.Hour() == 12 || times[t].ReferenceTime.Hour() == 18 { // forEach 6th hour

			if times[t].ReferenceTime.UTC().Minute() != 0 {
				continue
			}

			var iceThickness float32 = 0.0
			var waterThickness float32 = 0.0
			var snowThickness float32 = 0.0
			for o := 0; o < len(times[t].Observations); o++ {
				if times[t].Observations[o].Unit != "mm" { // Just in case ..
					log.Printf("GetFrostObses() Unsupported unit: %s", times[t].Observations[o].Unit)
					continue
				}
				if times[t].Observations[o].ElementID == "road_ice_thickness" {
					iceThickness = times[t].Observations[o].Value

				}
				if times[t].Observations[o].ElementID == "road_water_film_thickness" {
					waterThickness = times[t].Observations[o].Value
				}
				if times[t].Observations[o].ElementID == "road_snow_thickness" {
					snowThickness = times[t].Observations[o].Value
				}
			}

			if iceThickness == 0.0 && waterThickness == 0.0 && snowThickness == 0.0 {
				roadConditionClass = Dry

			} else if iceThickness > 0.0 || snowThickness > 0.0 {
				roadConditionClass = SnowAndOrIce // Can also be be water or no-water

			} else if waterThickness > 0.0 { // Can not be Snow an or Ice becasue ^
				roadConditionClass = Wet

			} else {
				panic("Logic error")
			}

			if roadConditionClass == Dry && (times[t].ReferenceTime.UTC().Hour() == 0 || times[t].ReferenceTime.UTC().Hour() == 12 ||
				times[t].ReferenceTime.UTC().Hour() == 6 || times[t].ReferenceTime.UTC().Hour() == 18) {
				obs := ObsRoadweather{}
				obs.CamID = sourcesMap[times[t].SourceID].ID
				obs.Station = times[t].SourceID
				obs.IceThickness = iceThickness
				obs.WaterFilmThickness = waterThickness
				obs.SnowThickness = snowThickness
				obs.RefTime = times[t].ReferenceTime.UTC()
				obs.Class = roadConditionClass
				obs.FrostID = times[t].SourceID

				class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				classesCount["Dry"]++
				//fmt.Printf("Obs: %+v\n", obs)

			} else if roadConditionClass == SnowAndOrIce && (times[t].ReferenceTime.UTC().Hour() == 0 || times[t].ReferenceTime.UTC().Hour() == 12 ||
				times[t].ReferenceTime.UTC().Hour() == 6 || times[t].ReferenceTime.UTC().Hour() == 18) { // With or without water
				obs := ObsRoadweather{}
				obs.CamID = sourcesMap[times[t].SourceID].ID
				obs.Station = times[t].SourceID
				obs.IceThickness = iceThickness
				obs.WaterFilmThickness = waterThickness
				obs.SnowThickness = snowThickness
				obs.RefTime = times[t].ReferenceTime.UTC()
				obs.Class = roadConditionClass

				class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)
				classesCount["SnowAndOrIceWithOrWithoutWater"]++
				obs.FrostID = times[t].SourceID
				//fmt.Printf("Obs: %+v\n", obs)

			} else if roadConditionClass == Wet && (times[t].ReferenceTime.UTC().Hour() == 0 || times[t].ReferenceTime.UTC().Hour() == 12 ||
				times[t].ReferenceTime.UTC().Hour() == 6 || times[t].ReferenceTime.UTC().Hour() == 18) {

				obs := ObsRoadweather{}
				obs.CamID = sourcesMap[times[t].SourceID].ID
				obs.Station = times[t].SourceID
				obs.IceThickness = iceThickness
				obs.WaterFilmThickness = waterThickness
				obs.SnowThickness = snowThickness
				obs.RefTime = times[t].ReferenceTime.UTC()
				obs.Class = roadConditionClass
				obs.FrostID = times[t].SourceID
				//fmt.Printf("Obs: %+v\n", obs)

				class2Obses[roadConditionClass] = append(class2Obses[roadConditionClass], obs)

				classesCount["Wet"]++
			}
		}
		from = from.Add(24 * time.Hour)
		to = to.Add(24 * time.Hour)
	}

	log.Printf("\nCLASSCOUNT: %+v", classesCount)

	return class2Obses, nil
}

func main() {
	db.DBFILE = "var/lib/roadlabels/roadcams.db"
	sources, err := GetStationsWithSensor()
	if err != nil {
		log.Printf("GetStationsWithSensor: %v", err)
	}

	keys := maps.Keys(sources)
	s := strings.Join(keys, ",")
	fmt.Printf("Souurces: %s\n", s)

	_, err = GetObsMapForLabelApp()
	if err != nil {
		fmt.Printf("getObses: %v\n", err)
		os.Exit(1)
	}

	/*

	 */
	/*
		GetDataFromFrost(sources)


	*/
}
