import os
import time
import threading
import multiprocessing
import queue as thread_queue
import random
import shutil

DIR_NAME = "test_files"
KEYWORDS = ['python', 'multiprocessing', 'threading', 'lorem']
NUM_FILES = 100
NUM_WORKERS = 4


def create_dummy_files():
    """Creates temporary files with random content and keywords."""
    shutil.rmtree(DIR_NAME, ignore_errors=True)
    os.makedirs(DIR_NAME, exist_ok=True)

    base_text = """
    Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
    Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. 
    Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. 
    Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. 
    Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
    """

    print(f"Creating {NUM_FILES} test files in '{DIR_NAME}'...")
    for i in range(NUM_FILES):
        file_path = os.path.join(DIR_NAME, f"file_{i:03d}.txt")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(base_text * random.randint(100, 500))

                if random.random() > 0.5:
                    f.write(f"\nSpecial word: {random.choice(KEYWORDS)}")
                if random.random() > 0.3:
                    f.write(f"\nAnother word: {random.choice(KEYWORDS)}")

        except IOError as e:
            print(f"Error creating file {file_path}: {e}")


def process_files_chunk(file_chunk, keywords):
    """
    Processes a chunk of files and searches for keywords in them.
    This function must be at the top level for multiprocessing to pickle it.
    """
    local_results = {k: [] for k in keywords}

    for file_path in file_chunk:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().lower()

                for keyword in keywords:
                    if keyword.lower() in content:
                        local_results[keyword].append(file_path)

        except (IOError, FileNotFoundError) as e:
            print(f"Error reading file {file_path}: {e}")
        except Exception as e:
            print(f"Unexpected error with file {file_path}: {e}")

    return local_results


def merge_results(partial_results, keywords):
    """Merges results from all workers into a single dictionary."""
    final_results = {k: [] for k in keywords}
    for res in partial_results:
        for keyword, files in res.items():
            final_results[keyword].extend(files)

    for keyword in final_results:
        final_results[keyword] = sorted(list(set(final_results[keyword])))

    return final_results


def generic_worker(file_chunk, keywords, results_queue):
    """
    Worker function for both threads and processes.
    Processes a chunk of files and puts the result in the queue.
    """
    result = process_files_chunk(file_chunk, keywords)
    results_queue.put(result)


def run_analysis(file_list, keywords, num_workers, mode):
    """
    Runs the file analysis using the specified parallelization mode.

    :param file_list: List of files to process.
    :param keywords: List of keywords to search for.
    :param num_workers: Number of threads/processes to use.
    :param mode: 'threading' or 'multiprocessing'.
    """
    if mode == 'threading':
        WorkerClass = threading.Thread
        QueueClass = thread_queue.Queue
        mode_name = "Threading"
    elif mode == 'multiprocessing':
        WorkerClass = multiprocessing.Process
        QueueClass = multiprocessing.Queue
        mode_name = "Multiprocessing"
    else:
        raise ValueError(f"Unknown mode: {mode}")

    print(f"\n--- Starting {mode_name} with {num_workers} workers ---")
    start_time = time.time()

    chunks = [file_list[i::num_workers] for i in range(num_workers)]
    results_queue = QueueClass()
    workers = []

    for chunk in chunks:
        worker = WorkerClass(target=generic_worker, args=(chunk, keywords, results_queue))
        workers.append(worker)
        worker.start()

    for worker in workers:
        worker.join()

    partial_results = []
    while not results_queue.empty():
        partial_results.append(results_queue.get())

    final_results = merge_results(partial_results, keywords)

    end_time = time.time()
    print(f"Execution time ({mode_name}): {end_time - start_time:.4f} seconds")
    return final_results


def main():
    """Main function to run the demonstration."""
    if __name__ == "__main__":
        try:
            create_dummy_files()

            file_list = [
                os.path.join(DIR_NAME, f)
                for f in os.listdir(DIR_NAME)
                if f.endswith('.txt')
            ]

            if not file_list:
                print("No files found to process.")
                return

            threading_results = run_analysis(file_list, KEYWORDS, NUM_WORKERS, 'threading')
            print("Results (Threading):")
            for k, v in threading_results.items():
                print(f"  - '{k}': found in {len(v)} files")

            processing_results = run_analysis(file_list, KEYWORDS, NUM_WORKERS, 'multiprocessing')
            print("Results (Multiprocessing):")
            for k, v in processing_results.items():
                print(f"  - '{k}': found in {len(v)} files")

            if threading_results == processing_results:
                print("\nVerification: Both methods produced the same result.")
            else:
                print("\nError: Results do not match.")

        finally:
            print(f"Removing temporary directory '{DIR_NAME}'...")
            shutil.rmtree(DIR_NAME, ignore_errors=True)
            print("Done.")


main()