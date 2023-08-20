import http.server
import urllib.parse
import queue
import threading
import time
import json
from functools import reduce
import urllib.request
import os

# Глобальные очереди для хранения задач сортировки и результатов
sorting_queue = queue.Queue()
result_queue = queue.Queue()

# Глобальный словарь для хранения состояний и данных сортировки
sorting_data = {}
sorting_data_lock = threading.Lock()


def merge(left, right):
    merged = []
    left_idx, right_idx = 0, 0

    while left_idx < len(left) and right_idx < len(right):
        if left[left_idx] < right[right_idx]:
            merged.append(left[left_idx])
            left_idx += 1
        else:
            merged.append(right[right_idx])
            right_idx += 1

    merged.extend(left[left_idx:])
    merged.extend(right[right_idx:])
    return merged


# Функция для выполнения сортировки слиянием
def merge_sort(numbers):
    if len(numbers) <= 1:
        return numbers

    # Функция для слияния двух отсортированных массивов

    # Разделяем массив на две части и рекурсивно сортируем каждую из них
    mid = len(numbers) // 2
    left_half = merge_sort(numbers[:mid])
    right_half = merge_sort(numbers[mid:])

    # Используем reduce для последовательного слияния всех пар массивов
    return reduce(merge, [left_half, right_half])


# Функция worker для сортировки кусков данных
def worker():
    while True:
        thread_name = threading.current_thread().name
        print(f"Worker thread: {thread_name}")

        query_params, job_id = sorting_queue.get()
        url = query_params.get("sort", [None])[0]
        concurrency = int(query_params.get("concurrency", [1])[0])

        if url is None:
            with sorting_data_lock:
                sorting_data[job_id] = {
                    "state": "error",
                    "data": []
                }
            sorting_queue.task_done()
            continue

        try:
            with urllib.request.urlopen(url) as response:
                data = response.read().decode("utf-8")
                numbers = [int(num) for num in data.strip().split()]
                chunk_size = len(numbers) // concurrency
                chunks = [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]

                result_chunks = []
                for chunk in chunks:
                    result_chunk = merge_sort(chunk)
                    result_chunks.append(result_chunk)

                result = reduce(merge, result_chunks)

                with sorting_data_lock:
                    sorting_data[job_id] = {
                        "state": "ready",
                        "data": result
                    }

                result_queue.put((result, job_id))
        except Exception as e:
            with sorting_data_lock:
                sorting_data[job_id] = {
                    "state": "error",
                    "data": []
                }
        finally:
            sorting_queue.task_done()


# Класс HTTP-обработчика для сервера
class SortingServerHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        thread_name = threading.current_thread().name
        print(f"Server thread: {thread_name}")


        if self.path.startswith("/?sort="):
            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            job_id = str(time.time()).replace(".", "")
            sorting_queue.put((query_params, job_id))

            with sorting_data_lock:
                sorting_data[job_id] = {
                    "state": "queued",
                    "data": []
                }
            self.send_response(200)
            self.end_headers()
            self.wfile.write(job_id.encode())

        elif self.path.startswith("/?get="):
            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            job_id = query_params.get("get", [None])[0]

            if job_id is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid parameters")
                return

            with sorting_data_lock:
                sort_info = sorting_data.get(job_id)

            if sort_info is None:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Job not found")
                return

            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(sort_info).encode())

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Page not found")


# Функция для запуска сервера
def start_server():
    server_address = ("", 8888)
    http_server = http.server.HTTPServer(server_address, SortingServerHandler)
    print("Server is running on port 8888")
    http_server.serve_forever()


if __name__ == "__main__":
    server_thread = threading.Thread(target=start_server)
    num_worker_threads = 2
    worker_threads = [threading.Thread(target=worker) for _ in range(num_worker_threads)]

    server_thread.start()
    for t in worker_threads:
        t.start()
