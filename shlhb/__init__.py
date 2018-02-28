from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()

# @sched.scheduled_job('interval', seconds=3)
# def timed_job():
#     print('This job is run every three minutes.')

@sched.scheduled_job('cron', day_of_week='mon-fri', hour='16-23')
def scheduled_job():
    print('This job is run every weekday at 5pm.')


if __name__=='__main__':
    print('before the start funciton')
    sched.start()
    print("let us figure out the situation")