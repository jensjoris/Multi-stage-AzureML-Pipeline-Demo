from datetime import datetime

def report_time(event: str):
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S:%f")
    print(f"{event}: ", current_time)
