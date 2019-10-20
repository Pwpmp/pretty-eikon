import eikon as ek  # the Eikon Python wrapper package
import re
import time
import datetime
import os
import pandas as pd
import eventlet
eventlet.monkey_patch()
#set_app_id function being deprecated
eikon_app_id = 'fb1f0ff6cf794376af6ff9f29926d3fb17997843' #paste real app id
ek.set_app_key(eikon_app_id)

print('Eikon connected')

#range of dates
start = datetime.datetime.strptime("2018-02-28", "%Y-%m-%d")
end = datetime.datetime.strptime("2019-06-1", "%Y-%m-%d")
date_generated = [start + datetime.timedelta(days=x) for x in range((end-start).days)][::-1]

rics = set(pd.read_csv('constituents_matched.csv', sep=',')['reuters'])
# #find not done yet
with open('done.txt', 'r') as f:
    notdone = set(f.read().strip().split('\n')[:-100])

rics -= notdone

print('Updating {} companies'.format(str(len(rics))))


def _handle_time(e, event):
    if hasattr(e, 'message'):
        mess = e.message.strip().split(' ')
        number = mess[-2]
        if number.isdigit():
            curr_time = datetime.datetime.now()
            number = int(number)
            print("It's now {}".format(curr_time.strftime('%Y-%m-%d %H:%M:%S')))
            print('going to sleep for {} hours'.format(datetime.timedelta(seconds=number)))
            print('Will be back {}'.format((curr_time + datetime.timedelta(seconds = number)).strftime('%Y-%m-%d %H:%M:%S')))
            event.cancel()
            time.sleep(number)
        else:
            print(ric, ' failed - ', e.message)
    else:
        print(ric, ' failed - ', e)


for ric in rics:
    for date in date_generated:
        with eventlet.Timeout(10,False) as event:
            try:
                news = ek.get_news_headlines(ric+' AND Language:LEN',
                         date_from=date.strftime("%Y-%m-%d"),
                         date_to=(date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                         count=100
                         )
            except Exception as e:
                _handle_time(e, event)
                continue

        for i in range(len(news)):
            time.sleep(0.2)
            storyId = news.iat[i,2]
            storyid = re.sub(':', '_', storyId)
            path = "{}\\{}\\{}\\{}\\{}.txt".format(date.year, date.month, date.day, ric, storyid)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # HTML story
            with eventlet.Timeout(10,False) as event:
                try:
                    story = ek.get_news_story(storyId)
                except Exception as e:
                    _handle_time(e, event)
                    print('failed for', storyId)
                    with open('failed.txt', 'a') as f:
                        f.write("{},{}\n".format(ric, storyId))
                    continue

            with  open(path, "w", encoding="utf-8") as f:
                f.write(ric + '\n') #Get company name
                f.write(date.strftime("%Y-%m-%d") + '\n') #Get date
                f.write(news.iat[i,3] + '\n') #Get newspaper company name
                f.write(story) #Get text
                print('Done', storyId)
                print(date.strftime("%Y-%m-%d"))
    with open('done.txt', 'a') as f:
        f.write(ric + '\n')
