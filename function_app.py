import logging
import azure.functions as func
import requests
from math import ceil
import os 

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = func.FunctionApp()

baseurl = os.getenv("BASEURL","")

def create_session():
    session = requests.Session()
    retry = Retry(
        total=5, 
        backoff_factor=1, 
        status_forcelist=[500, 502, 503, 504], 
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session

def getapi_data(page=1,data_type=None,session=None):
    endpoint = "getmovies" if data_type == "movie" else ("gettvshow" if data_type == "show" else None)
    try:
        if endpoint != None:
            logging.info(f"fetching for {data_type} page {page}")
            req_api_data = session.get(f"{baseurl}/api/{endpoint}.json?page={page}")
            req_api_data.raise_for_status()
            return req_api_data.json()
        else:
            raise Exception("data_type not found")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {data_type} api data for page {page}: {e}")
        return None

def getdb_data():
    try:
        req_db_data = requests.get(f"{baseurl}/api/dbdata.json")
        req_db_data.raise_for_status()
        return req_db_data.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching database data: {e}")
        return []

def fetch_additional_data(data_type,total_results,data_list,session,):
        pages = ceil(total_results / 20)
        
        logging.info(f"Fetching additional {pages} pages of {data_type} data.")
        for page in range(2, pages + 2): 
            new_data = getapi_data(page=page,data_type="movie",session=session)
            if new_data:
                data_list.extend(new_data["results"])

def filter_ids(data_type,data_list,sync_diff,session):
    list_subset = data_list[:sync_diff]
    id_list = [item["id"] for item in list_subset]
    try:
        response = session.post(
            f"{baseurl}/api/checkids.json?checkon={data_type}",
            json=id_list,
        )
        if response.status_code == 200:
            existing_ids = response.json()
            logging.info(f"Syncing {len(existing_ids)} new Ids to {data_type}")
            return [item for item in list_subset if item["id"] in existing_ids]
            
        else:
            logging.critical(f"Failed fetch new Ids")
    except requests.exceptions.RequestException as e:
        logging.critical(f"Error fetching data from db: {e}")


def post_data(data_type,data_list, sync_diff,session):
    items_to_sync = filter_ids(data_type=data_type,data_list=data_list,sync_diff=sync_diff,session=session)

    logging.info(f"Items to sync: {len(items_to_sync)}")

    if items_to_sync:
        for item in items_to_sync:
            data_to_post = {
                    "id": item["id"],
                    "title": item["title"],
                    "poster_path": item["poster_path"],
                    "vote_average": item["vote_average"],
                    "release_date": item["release_date"],
                } if data_type =="movie" else {
                        "id": item["id"],
                        "name": item["name"],
                        "poster_path": item["poster_path"],
                        "vote_average": item["vote_average"],
                        "first_air_date": item["first_air_date"],
                    }
            
            try:
                response = session.post(
                    f"{baseurl}/api/dbdata.json?insert={data_type}",
                    json=data_to_post,
                )
                if response.status_code == 200:
                    logging.info(f"Successfully synced {data_type}: {item['title'] if data_type == 'movie' else item.get('name', 'Unknown')}")
                else:
                    logging.error(f"Failed to sync {data_type} - {item['title'] if data_type == 'movie' else item.get('name', 'Unknown')} : {response.status_code}")
            except requests.RequestException as e:
                logging.error(f"Error in syncing {data_type} - {item['title'] if data_type == 'movie' else item.get('name', 'Unknown')} : {e}")



@app.timer_trigger(schedule="0 */15 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=True) 
def SyncTower(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info("Starting Sync Cron")

    if baseurl != "":
        movie_list = []
        show_list = []
        db_data = []
        total_results_movie = 0
        total_results_show = 0

        session = create_session()

        parsed_api_data = getapi_data(page=1,data_type="movie",session=session)
        if not parsed_api_data:
            return 
        total_results_movie = parsed_api_data["total_results"]
        movie_list.extend(parsed_api_data["results"])

        parsed_api_show_data = getapi_data(page=1,data_type="show",session=session)
        if not parsed_api_show_data:
            return
        total_results_show = parsed_api_show_data["total_results"]
        show_list.extend(parsed_api_show_data["results"])

        parsed_db_data = getdb_data()
        if not parsed_db_data:
            return
        
        sync_diff_movie = total_results_movie - parsed_db_data["movies"]
        sync_diff_show = total_results_show - parsed_db_data["shows"]

        logging.info(f"{sync_diff_movie},{sync_diff_show}")

        if sync_diff_movie > 20 : fetch_additional_data("movie",total_results_movie,movie_list,session=session) 
        if sync_diff_show > 20 : fetch_additional_data("show",total_results_show,show_list,session=session) 

        if len(movie_list) > 0 and sync_diff_movie > 0:
            post_data("movie",movie_list, sync_diff_movie,session=session)
        else:
            logging.info("No new movies to sync.")
        if len(show_list) > 0 and sync_diff_show > 0:
            post_data("show",show_list, sync_diff_show,session=session)
        else:
            logging.info("No new shows to sync.")

    logging.info('Python timer trigger function executed.')