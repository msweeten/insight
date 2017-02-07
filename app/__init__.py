from flask import Flask
app = Flask(__name__)
from app import views

import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from test import blah

cron = BackgroundScheduler()
cron.start()
cron.add_job(func=blah,trigger=IntervalTrigger(seconds=2),id='print',name='printblah',replace_existing=True)
atexit.register(lambda: cron.shutdown())


