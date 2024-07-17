import requests
import json

from fuzzywuzzy import fuzz, process

import time, copy
import os

#nel file addresses.txt bisogna inserire gli indirizzi da cercare.
#ogni indirizzo deve essere scritto su una riga
#nella forma:
    #Via X, altro...
    #cioè l'importante è che il nome sia scritto per prima e che sia seguita da una virgola,
    #la quale deve essere per la prima volta presente in quel momento

#get api key from file
with open('../keys/TomTom_key.txt', 'r') as file:
    tom_tom_api_key = file.read().strip()

with open('../keys/OpenWeather_key.txt', 'r') as file:
    open_weather_api_key = file.read().strip()

#function to read input from a json file and call the api
def get_addresses_info(file_name):

    with open(file_name, 'r') as file:
        addresses = file.readlines()

    output = []
    
    for address in addresses:

        result = api_calls(address.strip())

        if result is None:
            print(f"Error, address: '{address}' info not added")
            continue
        else:

            if len(result['elements']) == 0:
                print('No elements found', address)
                continue

            index = address.index(',')
            input_way_name = address[:index]

            result_filtered = json_filter(result, input_way_name)

            if result_filtered is None:
                print('No elements is matching the input', input_way_name, result)
                continue

            output.extend(result_filtered)

            print('Request completed successfully', address)

    return output


def api_calls(address):
    #find the latitude and longitude of the address
    tomtom_response = call_TomTom_api(address)

    if tomtom_response is None:
        return None
    
    lat = tomtom_response[0]
    lon = tomtom_response[1]

    query = f"""
        [out:json];
        way(around:600, {lat}, {lon})["name"]["highway"];
        out body;
        >;
        out skel qt;
    """
    overpass_response = call_overpass_api(query)

    if overpass_response is None:
        return None
    
    return overpass_response

def call_TomTom_api(address):
    
    url = f"https://api.tomtom.com/search/2/geocode/{address}.json"

    params = {
        'key': tom_tom_api_key
    }

    try:
        
        response = requests.get(url, params=params)

        response.raise_for_status()  # Raises HTTPError, if one occurred.
        api_response = response.json()  

        # Access the first result's position directly
        position = api_response['results'][0]['position']

        lat = position['lat']
        lon = position['lon']

        return [lat, lon]
    
    except requests.exceptions.RequestException as e:
        print(f'Error making TomTom API request: {e}')
        return None
    

def call_overpass_api(query):
   
    url = 'https://overpass-api.de/api/interpreter'

    try:
        
        response = requests.post(url, data=query)

        response.raise_for_status()  
        api_response = response.json()

        return api_response 

    except requests.exceptions.RequestException as e:
        print(f'Error making Overpass API request: {e}')
        return None


def json_filter(input_json, way_name):
        
    # Filter elements of type 'way'
    way_elements = [el for el in input_json['elements'] if el['type'] == 'way']
        
    # Compare tags->name with input string using fuzzywuzzy,
    #I use a tuple as key of the dictionary, since the streets
    #can be divided into multiple elements with the same "name" but different "id"
    names = {(el['id'] ,el['tags']['name']): el for el in way_elements}
    keys = names.keys()
    
    #transform the tuples into a dictionary that has as key the first element of the tuple and as value the second
    id_name_dict = {tupla[0]: tupla[1] for tupla in keys}
    
    best_match = process.extract(way_name, id_name_dict, scorer=fuzz.token_sort_ratio, limit=100) #get a list with tuples (match, score, match key)

    best_match_filtered = [el for el in best_match if el[1] >= 80]
    
    if best_match_filtered == []:
        return None
    
    best_match_name = best_match_filtered[0][0]

    way_ids = [el[2] for el in best_match_filtered if el[0] == best_match_name]

    # Get the elements with the highest score (same name so same score)
    best_elements = [names[(id, best_match_name)] for id in way_ids]

    #get array nodes from best_elements
    node_ids = [] #a list of lists...
    for i in range(len(best_elements)):
        node_ids.append(best_elements[i]["nodes"]) #we are adding a row, because we are adding a list

    node_elements = [] #a list of lists...
    for i in range(len(node_ids)):
        node_elements.append( [el for el in input_json['elements'] if el['type'] == 'node' and el['id'] in node_ids[i]] )

    output = []
    for i in range(len(best_elements)):
        combined_json = {
            "way_info": best_elements[i],
            "way_nodes_info": node_elements[i]
        }
        output.append(combined_json)

    return output

def filter_addresses_info(input):

    output = []

    for el in input:

        way_info = el["way_info"]
        data = {
            "way_id": way_info["id"],
            "highway": way_info["tags"]["highway"],
            "name": way_info["tags"]["name"]
        }
        way_nodes_info = el["way_nodes_info"]

        coppie = [(node["lat"], node["lon"]) for node in way_nodes_info]

        data["lista_coppie"] = coppie

        output.append(data)

    return output


def get_point_info(lat, lon):
    
    tags = ["highway=crossing", "highway=give_way", "railway", \
            "highway=stop", "traffic_calming", "highway=traffic_signals", "amenity", "noexit", "junction"]
    
    #create a dict with the tags as key and integer(...boolean) as value
    output_dict = {tag: 0 for tag in tags}

    query = f"""
        [out:json];
        (

        node(around:30, {lat}, {lon})[{tags[0]}];
        node(around:30, {lat}, {lon})[{tags[1]}];
        node(around:30, {lat}, {lon})[{tags[2]}];
        node(around:30, {lat}, {lon})[{tags[3]}];
        node(around:30, {lat}, {lon})[{tags[4]}];
        node(around:30, {lat}, {lon})[{tags[5]}];
        node(around:100, {lat}, {lon})[{tags[6]}];
        node(around:100, {lat}, {lon})[{tags[7]}];
        node(around:100, {lat}, {lon})[{tags[8]}];
        );
        out body;
        >;
        out skel qt;
    """

    overpass_response = call_overpass_api(query)

    if overpass_response is None:
        print("Error making Overpass API request for points of interest", lat, lon)
        return None
    else:

        #get all the elements
        elements = overpass_response['elements']
        
        #if there are no elements the cycle is not executed, but it is not a problem
        for el in elements:          

            tmp_tags = el['tags'] #for example {'traffic_calming': 'bump', 'highway': 'crossing'}

            for key, value in tmp_tags.items():
                k = key
                v = value
                k_v= k + "=" + v

                if k_v in tags:
                    output_dict[k_v] = 1
                elif k in tags:
                    output_dict[k] = 1

        #I have to create a dictionary equal to output_dict but with different keys
        #so I have to define a mapping
        mapping= {
            "highway=crossing": "Crossing", 
            "highway=give_way": "Give_Way", 
            "railway": "Railway", 
            "highway=stop": "Stop",
            "traffic_calming": "Traffic_Calming",
            "highway=traffic_signals": "Traffic_Signal",
            "amenity": "Amenity",
            "noexit": "No_Exit",
            "junction": "Junction"
        }

        output_dict = {mapping[k]: v for k, v in output_dict.items()}

        output_dict["lat"] = lat
        output_dict["lon"] = lon
        
        print("Output_dict of point " + str(lat) + " " + str(lon) + " is ", output_dict)
        return output_dict


def add_nodes_info(addresses_info):

    for address in addresses_info:

        data = {
            'nodes_info': []
        }

        for index, coppia in enumerate(address["lista_coppie"]):

            result = get_point_info(coppia[0], coppia[1])

            #add an element to the list nodes_info
            data['nodes_info'].append(result)

            time.sleep(1)

            if result is None:
                exit(1)

        address.update(data)

#remember that dictionaries are passed by reference
def insert_weather(input):

    units = "metric"

    output = copy.deepcopy(input)

    for el in output:
        
        #we take the first node of the road that will be the reference for the weather
        lat = el["lista_coppie"][0][0]
        lon = el["lista_coppie"][0][1]

        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units={units}&appid={open_weather_api_key}"

        try:
             
            response = requests.get(url)

            response.raise_for_status()  #Raises HTTPError, if one occurred

            data = response.json()

            if 'weather' in data and data['weather']:
                data = {"weather_info": data}
                el.update(data)
            else:
                print("No weather field found for the specified coordinates")
                return None
        
        except requests.exceptions.RequestException as e:
            print(f'Error making OpenWeather API request: {e}', lat, lon)
            return None
        
    return output


def check_logstash():

    #try to make a request until logstash is up
    while True:
        time.sleep(5)
        try:
        
            response = requests.get("http://logstash:9090")
            response.raise_for_status() 
            break

        except requests.exceptions.RequestException as e:
            print(f'Waiting for Logstash: {e}')

def send_to_logstash(json_string):

    try:
        
        response = requests.post("http://logstash:9090", data=json_string, \
                                        headers={"Content-Type": "application/json"})
        response.raise_for_status() 

    except requests.exceptions.RequestException as e:
        print(f'Error making request to Logstash: {e}')


if __name__ == '__main__':

    execution_mode = os.getenv('MY_MODE_VAR', 'Default') 

    if execution_mode == 'Demo':
        print('Demo mode')

        check_logstash()
        print('Logstash is up')

        umidity_values = [30, 40, 60]
        temperature_values = [-10, 20, 30]
        weather_icons = ["01d", "02d", "03d", "04d", "09d", "10d", "11d", "13d", "50d",
                             "01n", "02n", "03n", "04n", "09n", "10n", "11n", "13n", "50n"]
        wind_speed_values = [1, 8]
        pressure_values = [1010, 1020]          

        indexes = [(i, j, k, l, m) 
                            for i in range(0, len(umidity_values))
                            for j in range(0, len(temperature_values))
                            for k in range(0, len(weather_icons))
                            for l in range(0, len(wind_speed_values))
                            for m in range(0, len(pressure_values))]

        with open('../input_data/demo_mode/demo_input.jsonl', 'r') as file:

            for index_tuple in indexes:
                for line in file:

                    line_dict = json.loads(line)
                    line_dict["weather_info"]["dt"] = int(time.time())

                    line_dict["weather_info"]["main"]["humidity"] = umidity_values[index_tuple[0]]
                    line_dict["weather_info"]["main"]["temp"] = temperature_values[index_tuple[1]]
                    line_dict["weather_info"]["weather"][0]["icon"] = weather_icons[index_tuple[2]]
                    line_dict["weather_info"]["wind"]["speed"] = wind_speed_values[index_tuple[3]]
                    line_dict["weather_info"]["main"]["pressure"] = pressure_values[index_tuple[4]]
                    
                    line = json.dumps(line_dict)

                    send_to_logstash(line)
                
                time.sleep(5)
                file.seek(0) #to read the file from the beginning

    elif execution_mode == 'Default':
        print('Default mode')

        addresses_info = get_addresses_info("../input_data/addresses.txt")
        addresses_info = filter_addresses_info(addresses_info)

        add_nodes_info(addresses_info)
        #print(addresses_info)

        check_logstash()
        print('Logstash is up')

        while True:

            output = insert_weather(addresses_info)
            if output is None:
                time.sleep(60)
                continue

            for el in output:

                json_string = json.dumps(el)

                send_to_logstash(json_string)
            
            time.sleep(30)
        
    else:
        print('Unknown mode')
        exit(1)