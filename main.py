import http.server
import threading
import urllib.request
import os
import json
import queue
import time
from functools import reduce

# Глобальный словарь для хранения состояний и данных сортировки
sorting_data = {}
sorting_data_lock = threading.Lock()


# Функция для выполнения сортировки слиянием
def merge_sort(numbers):
    if len(numbers) <= 1:
        return numbers

    # Функция для слияния двух отсортированных массивов
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

    # Разделяем массив на две части и рекурсивно сортируем каждую из них
    mid = len(numbers) // 2
    left_half = merge_sort(numbers[:mid])
    right_half = merge_sort(numbers[mid:])

    # Используем reduce для последовательного слияния всех пар массивов
    return reduce(merge, [left_half, right_half])


# Класс HTTP-обработчика для сервера
class SortingServerHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/?sort="):
            # Обработка запроса на сортировку
            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            url = query_params.get("sort", [None])[0]
            concurrency = int(query_params.get("concurrency", [1])[0])

            if url is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid parameters")
                return

            # Генерируем уникальный jobid
            job_id = str(time.time()).replace(".", "")

            # Запускаем поток для обработки сортировки
            sorting_thread = threading.Thread(target=self.sort_numbers_thread, args=(url, concurrency, job_id))
            sorting_thread.start()

            # Сохраняем данные о задаче сортировки
            with sorting_data_lock:
                sorting_data[job_id] = {
                    "state": "queued",
                    "data": []
                }

            self.send_response(200)
            self.end_headers()
            self.wfile.write(job_id.encode())

        elif self.path.startswith("/?get="):
            # Обработка запроса на получение состояния сортировки
            query_params = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            job_id = query_params.get("get", [None])[0]

            if job_id is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Invalid parameters")
                return

            # Получаем данные о состоянии сортировки
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

    def sort_numbers_thread(self, url, concurrency, job_id):
        # Загружаем данные из URL и сохраняем их в локальный файл
        file_path = f"{job_id}.data"
        try:
            urllib.request.urlretrieve(url, file_path)
        except Exception as e:
            with sorting_data_lock:
                sorting_data[job_id] = {
                    "state": "error",
                    "data": []
                }
            return

        # Считываем данные из файла и сортируем
        with open(file_path, "r") as f:
            numbers = [int(num) for num in f.read().strip().split()]

        # Разбиваем данные на куски для многопоточной сортировки
        chunk_size = len(numbers) // concurrency
        chunks = [numbers[i:i + chunk_size] for i in range(0, len(numbers), chunk_size)]

        # Создаем список потоков для сортировки
        threads = []
        for chunk in chunks:
            t = threading.Thread(target=self.sort_chunk_thread, args=(chunk,))
            threads.append(t)
            t.start()

        # Ждем, пока все потоки завершат работу
        for t in threads:
            t.join()

        # Объединяем отсортированные куски
        sorted_numbers = reduce(merge, [t.result for t in threads])

        # Сохраняем результат сортировки
        with sorting_data_lock:
            sorting_data[job_id] = {
                "state": "ready",
                "data": sorted_numbers
            }

        # Удаляем временный файл
        os.remove(file_path)

    def sort_chunk_thread(self, numbers):
        result = merge_sort(numbers)
        threading.current_thread().result = result


# Очередь для хранения задач сортировки
sorting_queue = queue.Queue()

# Очередь для хранения результатов сортировки
result_queue = queue.Queue()


# Функция worker для сортировки кусков данных
def worker():
    while True:
        chunk, job_id = sorting_queue.get()
        result = merge_sort(chunk)
        result_queue.put(result)
        sorting_queue.task_done()


# Запускаем сервер в отдельном потоке
def start_server():
    server_address = ("", 8888)
    http_server = http.server.HTTPServer(server_address, SortingServerHandler)
    print("Server is running on port 8888")
    http_server.serve_forever()


if __name__ == "__main__":
    # Запускаем сервер и рабочий потоки
    server_thread = threading.Thread(target=start_server)
    num_worker_threads = 4  # Здесь можно указать количество worker-потоков
    worker_threads = [threading.Thread(target=worker) for _ in range(num_worker_threads)]
    server_thread.start()
    for t in worker_threads:
        t.start()
