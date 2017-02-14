from flask import Flask
app = Flask(__name__)
from app import views

import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import ap_update

cron = BackgroundScheduler()
cron.start()
#cron.add_job(func=blah,trigger=IntervalTrigger(seconds=2),id='print',name='printblah',replace_existing=True)
cron.add_job(func=ap_update.update, trigger = 'cron', day_of_week='sun', hour=0,id='update',name='update',replace_existing=True)
atexit.register(lambda: cron.shutdown())


