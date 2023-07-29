import http.server
import socketserver
import threading
import os
import json
import tempfile
import shutil
import http.client
import hashlib
import time
import random
import logging
import queue
import urllib.parse

# Инициализация логгера с указанием кодировки
logging.basicConfig(filename='server.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s',
                    encoding='utf-8')

# Общая очередь задач на сортировку
task_queue = []
task_queue_lock = threading.Lock()


# Функция сортировки для одного потока (QuickSort)
def quick_sort(data, low, high):
    if low < high:
        pivot = partition(data, low, high)
        quick_sort(data, low, pivot)
        quick_sort(data, pivot + 1, high)


def partition(data, low, high):
    pivot = data[(low + high) // 2]
    i = low - 1
    j = high + 1
    while True:
        i += 1
        while data[i] < pivot:
            i += 1
        j -= 1
        while data[j] > pivot:
            j -= 1
        if i >= j:
            return j
        data[i], data[j] = data[j], data[i]


def sort_data(data, result_queue):
    quick_sort(data, 0, len(data) - 1)
    result_queue.put(data)


# Функция для слияния отсортированных частей
def merge_data(chunks):
    result = []
    while chunks:
        min_chunk = min(chunks, key=lambda x: x[0])
        min_element = min_chunk[0]
        result.append(min_element)
        min_chunk.pop(0)
        if not min_chunk:
            chunks.remove(min_chunk)
    return result


# Функция для генерации случайных данных с числами
def generate_random_numbers():
    # Генерируем случайный размер массива чисел (от 10 до 100)
    size = random.randint(10, 100)

    # Генерируем случайные числа в диапазоне от 1 до 1000
    numbers = [random.randint(1, 1000) for _ in range(size)]
    return numbers


# Функция для записи данных во временный файл
def write_data_to_file(data):
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, "data.txt")
    with open(file_path, "w") as file:
        file.write(" ".join(map(str, data)))
    return file_path


# Функция для чтения данных из файла
def read_data_from_file(file_path):
    with open(file_path, "r") as file:
        data = list(map(int, file.read().split()))
    return data


# Функция для скачивания данных из URL и преобразования их в список чисел
def download_data_from_url(url):
    try:
        parsed_url = urllib.parse.urlparse(url)
        connection = http.client.HTTPSConnection(parsed_url.netloc)
        connection.request("GET", parsed_url.path)
        response = connection.getresponse()

        if response.status == 200:
            data = response.read().decode()
            return list(map(int, data.split()))
        else:
            logging.error(f"Failed to download data from URL: {url}")
            return None
    except Exception as e:
        logging.error(f"Error while downloading data from URL: {url}, Error: {str(e)}")
        return None


# Функция для обработки задачи
def process_task(concurrency, job_id, event, url=None):
    if url:
        # Скачиваем данные из указанного URL
        data = download_data_from_url(url)
        if data is None:
            # Если загрузка данных не удалась, возвращаем состояние ошибки
            with task_queue_lock:
                task_queue.append((job_id, {"state": "error", "message": "Failed to download data from the URL"}))
            event.set()
            return
    else:
        # Генерируем случайные данные для сортировки
        data = generate_random_numbers()

    # Записываем данные во временный файл
    file_path = write_data_to_file(data)

    # Читаем данные из файла и сортируем
    data = read_data_from_file(file_path)

    # Разбиение данных на порции для каждого потока сортировки
    chunk_size = len(data) // concurrency
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Запуск потоков сортировки
    threads = []
    result_queue = queue.Queue()
    for i in range(concurrency):
        thread = threading.Thread(target=sort_data, args=(chunks[i], result_queue))
        threads.append(thread)
        thread.start()

    # Ожидание завершения всех потоков
    for thread in threads:
        thread.join()

    # Получение отсортированных данных из очереди и объединение их
    sorted_chunks = []
    while not result_queue.empty():
        sorted_chunk = result_queue.get()
        sorted_chunks.append(sorted_chunk)

    # Слияние отсортированных частей
    sorted_data = merge_data(sorted_chunks)

    # Возвращение результата
    with task_queue_lock:
        task_queue.append((job_id, {"state": "ready", "data": sorted_data}))

    # Установка события завершения обработки задачи
    if job_id in job_id_to_event:
        job_id_to_event[job_id].set()

    # Удаление временного файла
    shutil.rmtree(os.path.dirname(file_path))


# Словарь для хранения объектов событий для каждой задачи
job_id_to_event = {}


# Класс HTTP-обработчика запросов
class SortHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith('/?concurrency='):
            # Извлечение параметров из URL
            params = urllib.parse.parse_qs(self.path[2:])
            concurrency = int(params.get('concurrency', [1])[0])

            # Генерация уникального job_id на основе хеша текущего времени
            job_id = hashlib.sha1(str(time.time()).encode()).hexdigest()

            # Создание объекта события для ожидания завершения обработки запроса на сортировку
            event = threading.Event()
            job_id_to_event[job_id] = event

            # Запуск потока для скачивания и обработки данных
            threading.Thread(target=process_task, args=(concurrency, job_id, event)).start()

            # Ожидание завершения обработки запроса на сортировку
            event.wait()

            # Возвращение job_id в HTTP-ответе
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(job_id.encode())

            # Логирование события
            logging.info(f"Новая задача с job_id={job_id} и concurrency={concurrency} запущена.")

        elif self.path.startswith('/?get='):
            job_id = self.path.split('=')[1]

            # Проверка наличия job_id в словаре job_id_to_event
            if job_id not in job_id_to_event:
                self.send_response(404)
                self.end_headers()
                return

            # Ожидание завершения обработки запроса на сортировку
            job_id_to_event[job_id].wait()

            # Поиск результата в общей очереди задач
            result = None
            with task_queue_lock:
                for task in task_queue:

                    if task[0] == job_id:
                        result = task[1]
                        break

            if result is None:
                # Если результата нет, отправляем 404 Not Found
                self.send_response(404)
                self.end_headers()
            else:
                # Если результат есть, отправляем его в ответе
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(result).encode())

            # Логирование события
            logging.info(f"Запрос для job_id={job_id} обработан со следующим результатом: {result}")

        elif self.path.startswith('/?url='):
            # Извлечение параметров из URL
            params = urllib.parse.parse_qs(self.path[2:])
            concurrency = int(params.get('concurrency', [1])[0])
            url = params.get('url', [None])[0]

            # Генерация уникального job_id на основе хеша текущего времени
            job_id = hashlib.sha1(str(time.time()).encode()).hexdigest()

            # Создание объекта события для ожидания завершения обработки запроса на сортировку
            event = threading.Event()
            job_id_to_event[job_id] = event

            # Запуск потока для скачивания и обработки данных
            threading.Thread(target=process_task, args=(concurrency, job_id, event, url)).start()

            # Ожидание завершения обработки запроса на сортировку
            event.wait()

            # Возвращение job_id в HTTP-ответе
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(job_id.encode())




            # Логирование события
            logging.info(f"Новая задача с job_id={job_id}, concurrency={concurrency}, и URL={url} запущена.")

        else:
            self.send_response(404)
            self.end_headers()

            # Логирование события
            logging.warning(f"Неверный запрос: {self.path}")


# Запуск HTTP-сервера в первом потоке
def start_http_server():
    port = 8888
    httpd = socketserver.TCPServer(("", port), SortHTTPRequestHandler)
    print(f"Сервер запущен на порту {port}")
    httpd.serve_forever()


# Запуск HTTP-сервера в отдельном потоке
http_server_thread = threading.Thread(target=start_http_server)
http_server_thread.start()
http_server_thread.join()
