import threading
import time
bg_tasks = {}

# Routine that processes whatever you want as background
def run_bg(method):
        global bg_tasks
        bg_tasks[method] = threading.Thread(target=method)
        temp = bg_tasks[method]
        temp.setDaemon(True)
        temp.start()
