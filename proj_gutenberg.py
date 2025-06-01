# Copyright 2025 Chang, Hsiao-Kang. All rights reserved.

# Standard library imports
import os
import re
import sys
import json
import time
import queue
import atexit
import threading
from time import sleep
from enum import IntEnum
from datetime import datetime
from http import HTTPStatus
from concurrent.futures import ThreadPoolExecutor

# Third-party imports
import requests as req
from bs4 import BeautifulSoup as bs

# -------------------------------------------
# Program start time
# -------------------------------------------
program_start_time = time.time()
program_start_timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
program_start_timestamp_for_file = datetime.now().strftime("%H-%M-%S_%f")[:-3]

# -------------------------------------------
# Feature control
# -------------------------------------------
file_save_origin_content = False    # Default: False
json_save_filtered_content = True   # Default: True
threaded_actual_download = True     # Default: True
max_book_download = 2000
# control if ThreadPoolExecutor is used
threadpool_enabled = True           # Default: True
# max_download_threads = 20
max_download_threads = os.cpu_count() * 2

json_file_in_target_dir = True
author_dir_enabled = False          # Default: False
show_duplicate_books = False        # Default: False
enable_tee_logging = True           # Log print to file

# -------------------------------------------
# Error Code Definitions
# -------------------------------------------
class ErrorCode(IntEnum):
    SUCCESS = 0
    GENERIC_ERROR = 1
    CONNECTION_ERROR = 2

# -------------------------------------------
# Configure setting
# -------------------------------------------
# Download over 200 Chinese books (better to be 400) from the Project Gutenberg
# https://www.gutenberg.org/browse/languages/zh
base_url = "https://www.gutenberg.org"
url = base_url + "/browse/languages/zh"
text_book_cache = base_url + "/cache/epub/" #"27119/pg27119.txt"
# text_url = "https://www.gutenberg.org/cache/epub/27119/pg27119.txt"
json_file_name = "gutenberg_books_zh.json"
json_dup_file_name = "gutenberg_books_zh_duplicate.json"
target_save_folder = "project_gutenberg"
teelogger_file_name = f"log_{program_start_timestamp_for_file}.txt"
logfile = None
logfile_lock = threading.Lock()
tee_logger = None

# -------------------------------------------
# global varables
# -------------------------------------------
duplicate_file_count = 0 
duplicate_books = {}
duplicate_lock = threading.Lock()
thread_log_active = False
log_queue = None
log_thread = None

my_headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0"
}

# TeeLogger supports multiple streams and have their own threading.Lock 
class TeeLogger:
    def __init__(self, *streams, lock_map=None):
        self.streams = list(streams)
        self.lock_map = lock_map or {}

    def write(self, data):
        for s in self.streams:
            lock = self.lock_map.get(s)
            try:
                if lock:
                    with lock:
                        s.write(data)
                else:
                    s.write(data)
            except UnicodeEncodeError:
                try:
                    s.write(data.encode("utf-8", errors="replace").decode("utf-8"))
                except Exception:
                    pass

    def flush(self):
        for s in self.streams:
            try:
                s.flush()
            except Exception:
                pass

    def close(self):
        for s in self.streams:
            try:
                if hasattr(s, "flush"):
                    s.flush()
                if hasattr(s, "close") and s not in (sys.__stdout__, sys.__stderr__):
                    s.close()
            except Exception:
                pass

def enable_tee_logger():
    global logfile, tee_logger_stdout, tee_logger_stderr
    if enable_tee_logging:
        logfile = open(teelogger_file_name, "w", encoding="utf-8", buffering=1)
        logfile_lock = threading.Lock()
        tee_logger_stdout = TeeLogger(sys.stdout, logfile, lock_map={logfile: logfile_lock})
        tee_logger_stderr = TeeLogger(sys.stderr, logfile, lock_map={logfile: logfile_lock})
        sys.stdout = tee_logger_stdout
        sys.stderr = tee_logger_stderr
        print(f"📜 Logging stdout and stderr to file: {teelogger_file_name}")        
        atexit.register(close_tee_logger)        

def close_tee_logger():
    global logfile, tee_logger_stdout, tee_logger_stderr
    if enable_tee_logging and tee_logger:
        print("Closeing log file")
        tee_logger_stdout.close()
        tee_logger_stderr.close()

def format_time_to_timestamp(seconds: float) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours):02}:{int(minutes):02}:{seconds:06.3f}"

def make_target_author_dir(author: str) -> str:
    if author_dir_enabled and author:
        folder_path = os.path.join(target_save_folder, author)
        strip_author = author.strip()
        if strip_author != author:
            print("Author name has spaces")
            breakpoint()
    else:
        # if author can't be found, let's not create it for now
        folder_path = target_save_folder
    if not os.path.exists(folder_path):
        try:
            os.mkdir(folder_path)  
        except FileExistsError:
            # We had antoher thread created it about the same time
            pass    
    # always return creatd path for later file creation
    return folder_path

def make_target_dir():
    if not getattr(make_target_dir, "_checked", False):
        if not os.path.exists(target_save_folder):
            try:
                os.makedirs(target_save_folder, exist_ok=True)
            except FileExistsError:
                # We had antoher thread created it about the same time
                pass
        make_target_dir._checked = True

# return ending positon of searched text
def search_re_value_cont(keyword, text, add_val_pattern="", postfix=""):
    re_kw_str = rf'{keyword} *([\w\. ,\-\u4E00-\u9FFF\u3000-\u303F\uFF00-\uFFEF{add_val_pattern}]+){postfix}'
    re_match = re.search(re_kw_str, text)
    if re_match:
        # print(f'Found {keyword} {re_match.group(1)}')
        return re_match.group(1).strip(), text[re_match.end():], text[:re_match.start()]
    else:
        return None, text[0:], text[0:]

def search_re_value_pos(keyword, text, add_val_pattern="", postfix=""):
    re_kw_str = rf'{keyword}\s*([\w \u4E00-\u9FFF\u3000-\u303F\uFF00-\uFFEF{add_val_pattern}]+){postfix}\n'
    re_match = re.search(re_kw_str, text)
    if re_match:
        # print(f'Found {keyword} {re_match.group(1)}')
        return re_match.group(1), re_match.start(), re_match.end()
    else:
        return None, 0, 0

'''
*** START OF THE PROJECT GUTENBERG EBOOK <Book Title> ***
Produced by Author

Content to keep is here....

*** END OF THE PROJECT GUTENBERG EBOOK <Book Title> ***
'''
def parse_book_text(book_text):
    book_info = {}

    book_title, book_search_text, found_pos = search_re_value_cont('Title:', book_text)
    book_author, book_search_text, found_pos = search_re_value_cont('Author:', book_search_text)
    book_editor, book_search_text, found_pos = search_re_value_cont('Editor:', book_search_text)
    book_release_date, book_search_text, found_pos = search_re_value_cont('Release date:', book_search_text, ',')
    book_lang, book_search_text, found_pos = search_re_value_cont('Language:', book_search_text)
    book_producer, book_search_text, text_start_ignored = search_re_value_cont('Produced by', book_search_text)    

    re_kw_str = rf"\*{{3}} START OF THE PROJECT GUTENBERG EBOOK (.+?) \*{{3}}"
    match_start_flag = re.search(re_kw_str, book_text)
    if match_start_flag:
        # print(f'Found start_flag {match_start_flag[1]}')
        pos_body_text_start = match_start_flag.end()

    re_kw_str = rf"\*{{3}} END OF THE PROJECT GUTENBERG EBOOK (.+?) \*{{3}}"
    match_end_flag = re.search(re_kw_str, book_text)
    if match_end_flag:
        # print(f'Found end flag {match_end_flag[1]}')
        pos_body_text_end = match_end_flag.start()

    if match_start_flag and match_end_flag:
        book_body_text = book_text[pos_body_text_start:pos_body_text_end]

    re_str = (
        # r'(["\u4E00-\u9FFF\u3000-\u303F\uFF00-\uFFEF]+)' 
        r'(["\u4E00-\u9FFF\u3000-\u303F\uFF01-\uFF0F\uFF1A-\uFF20\uFF3B-\uFF40\uFF5B-\uFF65]+)'
    )

    matches = re.findall(re_str, book_body_text)
    # for i in range(len(matches)):
        # print(f'Matches[{i}]: {matches[i]} ')
    chinese_only_text = '\n'.join(matches)
    book_info = {
        "title": book_title,
        "author": book_author,
        "lang": book_lang,
        "producer": book_producer,
        "editor": book_editor,
        "content": chinese_only_text
    }
    return book_info


def make_book_text_url(book_id):
    # text_url = "https://www.gutenberg.org/cache/epub/27119/pg27119.txt"
    text_file_url = text_book_cache + book_id + "/pg" + book_id + ".txt"
    # print(f'Book ID: {book_id}')
    # print(f'Book text url: {text_file_url}')
    return text_file_url

def get_unique_filename(filepath):
    global duplicate_file_count
    base, ext = os.path.splitext(filepath)
    counter = 1
    original = filepath
    while os.path.exists(filepath):
        filepath = f"{base}-{counter}{ext}"
        counter += 1
    if filepath != original:
        duplicate_file_count += 1
        msg = f"🔁 Duplicate filename detected. Saved as: {filepath}"
        if thread_log_active:
            thread_safe_log(msg)
        else:
            print(msg)
    return filepath

def download_text(text_url, book_title, book_id):
    try:
        res = req.get(text_url, headers=my_headers)
        if res.status_code != HTTPStatus.OK:
            http_status = HTTPStatus(res.status_code)
            print(f"Book title: {book_title} Book ID: {book_id} URL: {text_url}")
            print(f"HTTP Status code:{http_status.value} {http_status.phrase} {http_status.description}")        
            raise ConnectionError
    except ConnectionError as e:
        print(f"ConnectionError: {e}")        
        print("Please check your internet connection!\n")
        raise ConnectionError
    except Exception as e:
        # Catches all other unexpected errors
        print(f"Unexpected error:\n{e}")
        raise RuntimeError
        
    url_html = res.text
    book_info = {}
    book_info['url'] = text_url
    parsed_book_info = parse_book_text(url_html)
    book_info = {**book_info, **parsed_book_info}
    book_title_in_text = book_info["title"]
    if book_title != book_title_in_text:
        print(f'WARNING: Book title "{book_title}" mismatch with the one "{book_title_in_text}" in the ebook text!')

    folder_path = make_target_author_dir(book_info["author"])
    filename_base = book_title + '_' + book_id
    # filename_base = book_title_in_text + '_' + book_id
    filename = filename_base + '.txt'

    target_save_text = os.path.join(folder_path, filename)
    original_save_text = os.path.join(folder_path, filename.replace(".", "_old."))

    if file_save_origin_content:
        original_save_text_unique = get_unique_filename(original_save_text)
        with open(original_save_text_unique, "w", encoding="utf-8") as file:
            file.write(res.text)

    book_text = book_info["content"]
    target_save_text_unique = get_unique_filename(target_save_text)
    if target_save_text_unique != target_save_text:
        with duplicate_lock:
            if filename_base in duplicate_books:
                duplicate_books[filename_base] += 1
            else:
                duplicate_books[filename_base] = 1

    with open(target_save_text_unique, "w", encoding="utf-8") as file:
        file.write(book_text)

    return book_info

# ============================================================
# Threaded handling and thread-safe queue log
# ============================================================
def start_thread_log():
    global log_queue, log_thread, thread_log_active
    log_queue = queue.Queue()
    log_thread = threading.Thread(target=log_consumer)
    log_thread.start()
    thread_log_active = True

def thread_safe_log(msg: str):
    log_queue.put(msg)

def end_thread_log():
    global thread_log_active 
    thread_log_active = False
    log_queue.put("STOP")
    log_thread.join()

def log_consumer():
    while True:
        message = log_queue.get()
        if message == "STOP":
            break
        print(message)

def threaded_download_worker(book_with_index: tuple) -> tuple:
    index, book = book_with_index
    book_id = book["book_id"]
    book_title = book["title"]
    thread_id = threading.get_ident()
    start_time = time.time()
    elapse_time = start_time - program_start_time
    elapse_timestamp = format_time_to_timestamp(elapse_time)
    timestamp_start = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    thread_safe_log(f"[{timestamp_start}] [{elapse_timestamp}] 📘 [{index}] | Book ID: {book_id} | Book Title: {book_title}")

    successful_download = False
    if threaded_actual_download:
        try:
            book_info = download_text(make_book_text_url(book["book_id"]), book["title"], book["book_id"])
            successful_download = True
        except Exception as e:
            print(f"[ERROR] Failed to download book {book['book_id']}: {e}")
            book_info = {"content": None}        
    else:        
        thread_safe_log(f"[{timestamp_start}] Skip the download. Index: {index} | Book ID: {book_id} | Book Title: {book_title}")

    end_time = time.time()
    elapse_time = end_time - program_start_time
    elapse_timestamp = format_time_to_timestamp(elapse_time)    
    timestamp_end = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    duration = f"{(end_time - start_time):.3f}s"
    if successful_download:
        thread_safe_log(f"[{timestamp_end}] [{elapse_timestamp}] ✅ [{index}] | Book ID: {book_id} | Book Title: {book_title} | Done: Time: {duration}")
    else:
        thread_safe_log(f"[{timestamp_end}] [{elapse_timestamp}] ❌ [{index}] | Book ID: {book_id} | Book Title: {book_title} | Failed: Time: {duration}")

    if threaded_actual_download:
        return index, book_info, successful_download
    else:
        # Simulate info
        book["info"] = {
            "url": f"https://www.gutenberg.org/cache/epub/{book["book_id"]}/pg{book["book_id"]}.txt",
            "title": ""
        }     
        return index, book, True

def report_duplicate_books(duplicate_books_dict):
    if show_duplicate_books:
        print(f' Count | BookTitle_ID ') 
        print(f'-------+--------------') 
        for book in duplicate_books_dict:
            print(f' {duplicate_books_dict[book]:>4}  | {book} ') 
        print(f'Total Books: {len(duplicate_books)}')        

    json_dup_file_path_name = os.path.join(target_save_folder, json_dup_file_name)

    print(f'Saving JSON file "{json_dup_file_path_name}"...')
    with open(json_dup_file_path_name, "w", encoding="utf-8") as file:
        try:
            file.write(json.dumps(duplicate_books_dict, ensure_ascii=False, indent=4))
        except Exception as e:
            print(f"[ERROR] Failed to write JSON file {json_dup_file_path_name} ! : {e}")
            raise IOError   
        
def main():
    # First thing to do in main to log all print
    enable_tee_logger()
    make_target_dir()
    download_start_time = time.time()

    try:
        res = req.get(url, headers=my_headers)
        if res.status_code != HTTPStatus.OK:
            http_status = HTTPStatus(res.status_code)
            print(f"Book title: {book_title} Book ID: {book_id} URL: {url}")
            print(f"HTTP Status code:{http_status.value} {http_status.phrase} {http_status.description}")        
            return ErrorCode.CONNECTION_ERROR
    except ConnectionError as e:
        print(f"Connection Error:\n{e}")        
        print("\nPlease check your internet connection!\n")
        return ErrorCode.CONNECTION_ERROR       
    except Exception as e:
        # Catches all other unexpected errors
        print(f"Unexpected error:\n{e}")
        print("\nPlease check your internet connection!\n")
        return ErrorCode.CONNECTION_ERROR

    # print(res.text)
    soup = bs(res.text, "lxml")
    list_books = []

    count_book = 0
    for book in soup.select('li.pgdbetext > a[href]'):
        book_id = book["href"].split('/')[-1]
        book_title = book.get_text()
        # handle special two lines and other misc. cases
        book_title = re.sub(r'[\r\\/\?\*:<>\"\|]+', ' ', book_title)
        book_url = base_url + book["href"]
        list_books.append(
            {
                "index": count_book,
                "title": book_title,
                "book_id": book_id,
                "link": book_url
            }
        )
        count_book += 1

        # break early for few tests only
        if count_book == max_book_download:
            break

    print(f"Found {len(list_books)} books!")

    success_count = 0
    if threadpool_enabled:
        print("=== Enable threaded downloading ===")
        # Starts thread-safe log queue
        start_thread_log()

        with ThreadPoolExecutor(max_workers=max_download_threads) as pool:
            # Add index, each book becomes (index, book)
            indexed_books = list(enumerate(list_books))
            result_books = list(pool.map(threaded_download_worker, indexed_books))

            # Write back to original list_books
            for index, updated_book in enumerate(result_books):
                list_books[index]["info"] = updated_book

            for index, updated_book, success in result_books:
                if success:
                    list_books[index]["info"] = updated_book
                    success_count += 1
                else:
                    list_books[index]["info"] = updated_book

        # Ends thread-safe log queue
        end_thread_log()
    else:
        for index, book in enumerate(list_books):
            book_id = book["book_id"]
            book_title = book["title"]            
            start_time = time.time()
            elapse_time = start_time - program_start_time
            elapse_timestamp = format_time_to_timestamp(elapse_time)            
            timestamp_start = datetime.now().strftime("%H:%M:%S.%f")[:-3]            
            print(f"[{timestamp_start}] [{elapse_timestamp}] 📘 [{index}] | Book ID: {book_id} | Book Title: {book_title}")

            success = False
            try:
                book_info = download_text(make_book_text_url(book["book_id"]), book["title"], book
                ["book_id"])
                success_count += 1
                success = True
            except Exception as e:
                print(f"[ERROR] Failed to download book {book['book_id']}: {e}")
                book_info = {"content": None}

            end_time = time.time()
            elapse_time = end_time - program_start_time
            elapse_timestamp = format_time_to_timestamp(elapse_time)               
            timestamp_end = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            duration = f"{(end_time - start_time):.3f}s"

            if success:
                print(f"[{timestamp_end}] [{elapse_timestamp}] ✅ [{index}] | Book ID: {book_id} | Book Title: {book_title} | Done: Time: {duration}")
            else:
                print(f"[{timestamp_end}] [{elapse_timestamp}] ❌ [{index}] | Book ID: {book_id} | Book Title: {book_title} | Failed: Time: {duration}")  

            list_books[index]["info"] = book_info

            # break when reach few test books
            if index == max_book_download:
                break

    download_end_time = time.time()
    download_duration = f"{(download_end_time - download_start_time):.3f}s"
    print(f'Total Download Time: {download_duration}')
    print(f"✅📘 Total books downloaded successfully: {success_count} / {len(list_books)}")
    if duplicate_file_count > 0:
        print(f"✅📘🔁 Duplicate books downloaded: {duplicate_file_count}")
        try:
            report_duplicate_books(duplicate_books)
        except Exception as e:
            # Don't make this a critical error.
            print(f'ERROR: Duplicate list JSON file cannot be created!')

    if not json_save_filtered_content:
        # clean up content
        for index, book in enumerate(list_books):
            # del list_books[index]["info"]["content"]
            print(f'index {index}: link:{list_books[index]["link"]}')
            if book:
                if 'info' in book:
                    if 'content' in book["info"]:
                        del list_books[index]["info"]["content"]
            else:
                print(f'WARNING!!!  index {index}: link:{list_books[index]["link"]} title:{list_books[index]["title"]}')
    
    if json_file_in_target_dir:
        json_file_path_name = os.path.join(target_save_folder, json_file_name)
    else:
        json_file_path_name = json_file_name

    print(f'Saving JSON file "{json_file_path_name}"...')
    with open(json_file_path_name, "w", encoding="utf-8") as file:
        try:
            file.write(json.dumps(list_books, ensure_ascii=False, indent=4))
        except Exception as e:
            print(f"[ERROR] Failed to write JSON file {json_file_path_name}! : {e}")
            return ErrorCode.GENERIC_ERROR
    
    return ErrorCode.SUCCESS

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)    



