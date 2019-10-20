import pandas as pd
import eikon as ek
import time
import datetime
import re
import os
import eventlet
import json
from bs4 import BeautifulSoup

class EikonBase():
    def __init__(self,
                eikon_app_key,
                data,
                outputdir,
                iscontinue = False,
                pathtodone = None):
        """

        data :: [Str]        List with company rics
        outputdir :: Str Path to outputdirname
        iscontinue :: Bool    If true, won't get
                                        headlines for dates which already exist
        pathtodone :: Str Path to textfile with done rics

        dateto: :: Str  | datetime      Ending date, if Str form YYYY-MM-DD
        datefrom :: Str | datetime        Starting date, if Str form YYYY-MM-DD
        """
        ek.set_app_key(eikon_app_key)
        print('Eikon connected')
        eventlet.monkey_patch()
        self.ordered = ['A', 'O','B', 'K', 'PK'] #exchange types
        self.rics = data
        self.outputdir = outputdir
        self.iscontinue = iscontinue
        os.makedirs(self.outputdir, exist_ok =True)
        self.done = pathtodone
        if self.done is not None:
        #READ ALL COMPS ALREADY DONE
            with open(self.done, 'r') as f:
                done_rics = set(f.read().strip().split('\n'))
            self.rics -= done_rics

        print('Updating %d companies' % len(self.rics))

    def _handle_time(self, e, event):
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
                print('failed - ', e.message)
        else:
            print('failed - ', e)


class TimeSeries(EikonBase):
    def __init__(self,
                eikon_app_key,
                data,
                outputdir,
                iscontinue = False,
                pathtodone = None,
                dateto = datetime.datetime.now(),
                datefrom = None):

        super().__init__( eikon_app_key = eikon_app_key,
                        data = data,
                        outputdir = outputdir,
                        iscontinue = iscontinue,
                        pathtodone = pathtodone)

        if datefrom is None:
            datefrom = dateto - datetime.timedelta(weeks = 52)
        self.start = datetime.datetime.strptime(datefrom,"%Y-%m-%d")  \
                        if isinstance(datefrom, str) else datefrom

        self.end = datetime.datetime.strptime(dateto,"%Y-%m-%d")  \
                        if isinstance(dateto, str) else dateto

    def get_symbols(self, pathtocsv):
        """
        pathtocsv :: Str        path to csv file with comp denoting
                                column with company codes

        returns:
        Updates csv with col eikon, the rics for timeseries extraction
        """
        df = pd.read_csv(pathtocsv)
        symbols = set(df['comp'])
        date = datetime.datetime.now() #any date

        matched = {}
        fullen = len(symbols)
        for i, symbol in enumerate(symbols):
            matched[symbol] , succ = self.time_company(symbol)
            if not succ :
                print('%s not found' % symbol)
                continue

            with open(os.path.join(self.outputdir, 'matched.csv'), 'a') as f:
                f.write('{},{}\n'.format(symbol, matched[symbol])) #just in case
            print('done: {}/{}'.format(i, fullen))

        df['eikon'] = df['comp'].map(matched)
        df.to_csv(os.path.join(self.outputdir, 'reports_matched.csv'), mode = 'w', header = True, index = False)


    def time_import(self):
        #doesnt account for quota limit
        fullen = len(self.rics)
        for i, comp in enumerate(self.rics):
                try:
                    ric, prices = self.time_company(comp)
                    print('{} done ({}/{})'.format(ric, i, fullen))
                    if prices == {}:
                        with open('timeseries_failures.txt', 'a') as f:
                            f.write('{}\tNA\n'.format(ric))
                            continue
                    path =  "{}\\{}_{}.json".format(self.outputdir, comp, ric)
                    with  open(path, "w") as f:
                         json.dump(prices, f)
                except Exception as e:
                    print(comp, ' failed', 'exception: ', e)
                    with open('timeseries_failures.txt', 'a') as f:
                        f.write('{}\t{}\n'.format(comp,e))
                    continue



    def time_company(self, ric, number = -1):

        """
        symbol :: Str       Stock exchange company code, such as AAPL
        """

        if number == len(self.ordered) - 1:
            try:
                prices = ek.get_timeseries(ric,
                                           start_date=self.start,
                                           end_date=self.end,
                                           raw_output=True
                                           )
                return ric, prices

            except:
                print('{} failed'.format(ric))
                return ric, {}
        else:
            try:
                prices = ek.get_timeseries(ric,
                                           start_date=self.start,
                                           end_date=self.end,
                                           raw_output=True
                                           )
                return ric, prices

            except:
                time.sleep(0.2)
                number += 1
                ric = ric.split('.')[0] + '.'+ self.ordered[number]
                return self.time_company(ric, number)


class NewsProvider(EikonBase):
    def __init__(self,
                eikon_app_key,
                data,
                outputdir = 'news',
                iscontinue = False,
                pathtodone = None,
                dateto = datetime.datetime.now(),
                datefrom = None):

        super().__init__(eikon_app_key = eikon_app_key,
                        outputdir = outputdir,
                        data = data,
                        iscontinue = iscontinue,
                        pathtodone = pathtodone)
        if datefrom is None:
            datefrom = dateto - datetime.timedelta(days = 30 * 15)
        if self.done is None:
            self.done = os.path.join(outputdir, 'done.txt')

        start = datetime.datetime.strptime(datefrom,"%Y-%m-%d")  \
                        if isinstance(datefrom, str) else datefrom

        end = datetime.datetime.strptime(dateto,"%Y-%m-%d")  \
                        if isinstance(dateto, str) else dateto


        self.date_generated = [start + datetime.timedelta(days=x) for x in range((end-start).days)][::-1]

    def do_day(self, date, ric, do_clean):
        pathtodir = "{}\\{}\\{}\\{}\\{}".format(self.outputdir, date.year, date.month, date.day, ric)
        with eventlet.Timeout(10,False) as event:
            try:
                news = ek.get_news_headlines(ric+' AND Language:LEN',
                         date_from=date.strftime("%Y-%m-%d"),
                         date_to=(date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                         count=100
                         )
                self.process_headlines(news, pathtodir, date, ric, do_clean)
            except Exception as e:
                self._handle_time(e, event)

    def mine_news(self, do_clean = True):
        """
        Main method for mining news

        do_clean :: Bool    If True, preprocess raw html news to text
        """

        for ric in self.rics:
            for date in self.date_generated:
                self.do_day(date, ric, do_clean = do_clean)
            with open(os.path.join(self.outputdir, 'done.txt'), 'a') as f:
                f.write(ric + '\n')

    def process_headlines(self, news, pathtodir, date, ric, do_clean = True):
        """
        Writes news with dir structure
        +YYYY
            +MM
                +DD
                    +COMP
        """
        for i in range(len(news)):
            time.sleep(0.2)
            storyId = news.iat[i,2]
            storyid = re.sub(':', '_', storyId)
            path = os.path.join(pathtodir, storyid + '.txt')
            os.makedirs(pathtodir, exist_ok=True)
            if os.path.exists(path):
                print('Already exists, skipping.')
                continue
            # HTML story
            with eventlet.Timeout(10,False) as event:
                try:
                    story = ek.get_news_story(storyId)
                except Exception as e:
                    self._handle_time(e, event)

            if do_clean:
                story = self._text_cleaner(story)

            with  open(path, "w", encoding="utf-8") as f:
                f.write(ric + '\n') #Get company name
                f.write(date.strftime("%Y-%m-%d") + '\n') #Get date
                f.write(news.iat[i,3] + '\n') #Get newspaper company name
                f.write(story) #Get text
                print('Done', storyId)
                print(date.strftime("%Y-%m-%d"))

    def html_cleaner(self, pathtodir, destindir):
        """
        pathtodir:: Either a directory or a filename
        destindir:: Destination directory. Will mimic pathtodir tree structure
        """
        wrap_clean = lambda pathtofile: self._file_cleaner(pathtofile, destindir)
        allfiles = []
        for root, dirs, files in os.walk(pathtodir):
            for file in files:
                if file.endswith(".txt"):
                    allfiles.append(os.path.join(root, file))

        lenall = len(allfiles)
        if len(allfiles) > 1:

            print('Cleaning %d files' % lenall)

            threads = multiprocessing.cpu_count()
            pool = ThreadPool(threads)
            print('Using %d thread%s' % (threads, 's' if threads > 1 else ''))
            for _ in tqdm.tqdm(pool.imap_unordered(wrap_clean, allfiles), total = lenall):
                pass

        else:
            wrap_clean(allfiles[0])

    def _file_cleaner(self, pathtofile, destindir):
        """
        Clean an already existing file
        """
        with open(pathtofile, 'r') as f:
            f = f.read().splitlines()
            try:
                comp, date, source = f[:3]
                raw = " ".join(f[3:])
            except:
                return
        comp = comp.replace('.', '_')
        filename = os.path.basename(pathtofile).split('_')[3:]
        filename = "_".join([comp] + filename)
        destinpath = os.path.join(destindir, filename)
        text = self._text_cleaner(raw)
        if len(text.encode('utf-8')) > 50:
            with open(destinpath, 'w') as f:
                f.write(text)

    def _text_cleaner(self, raw):

        soup = BeautifulSoup(raw, features="html.parser")

        unnecessary = ("script",
                       "style",
                       ".tr-copyright",
                       ".tr-signoff",
                       ".tr-advisory",
                       ".tr-by",
                       ".tr-dateline",
                       ".tr-dl-sep",
                       ".tr-contactinfo",
                       ".tr-slugline",
                       ".tr-link",
                       ".tr-image",
                       ".tr-npp-lead",
                       )

        for b in unnecessary:
            try:
                soup.select_one(b).decompose()
            except Exception as e:
                continue

        text = soup.get_text(separator=' ')

        #remove multiple spaces
        text = " ".join(text.split())
        return text
