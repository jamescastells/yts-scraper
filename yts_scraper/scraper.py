import os
import sys
import math
import json
import csv
from concurrent.futures.thread import ThreadPoolExecutor
import requests
from tqdm import tqdm
from fake_useragent import UserAgent

class Scraper:
    """
    Scraper class.

    Must be initialized with args from argparser
    """
    # Constructor
    def __init__(self, args):
        self.output = args.output
        self.genre = args.genre
        self.minimum_rating = args.rating
        self.quality = '3D' if (args.quality == '3d') else args.quality
        self.categorize = args.categorize_by
        self.sort_by = args.sort_by
        self.year_limit = args.year_limit
        self.page_arg = args.page
        self.poster = args.background
        self.imdb_id = args.imdb_id
        self.multiprocess = args.multiprocess
        self.csv_only = args.csv_only
        self.view = args.view
        self.text = args.text

        self.movie_count = None
        self.url = None
        self.existing_file_counter = None
        self.skip_exit_condition = None
        self.downloaded_movie_ids = None
        self.pbar = None

        # Set output directory

        if args.view == False:

            if args.output:
                if not args.csv_only:
                    os.makedirs(self.output, exist_ok=True)
                self.directory = os.path.join(os.path.curdir, self.output)
            else:
                if not args.csv_only:
                    if (self.categorize != 'none'):
                        os.makedirs(self.categorize.title(), exist_ok=True)
                if (self.categorize != 'none'):
                    self.directory = os.path.join(os.path.curdir,self.categorize.title())
                else:
                    self.directory = os.path.curdir

        # Args for downloading in reverse chronological order
        if args.sort_by == 'latest':
            self.sort_by = 'date_added'
            self.order_by = 'desc'
        else:
            self.order_by = 'asc'


        # YTS API has a limit of 50 entries
        self.limit = 50


    # Connect to API and extract initial data
    def __get_api_data(self):
        # Formatted URL string
        url = '''https://yts.mx/api/v2/list_movies.json?quality={quality}&genre={genre}&minimum_rating={minimum_rating}&sort_by={sort_by}&query_term={text}&order_by={order_by}&limit={limit}&page='''.format(
            quality=self.quality,
            genre=self.genre,
            minimum_rating=self.minimum_rating,
            sort_by=self.sort_by,
            text=self.text,
            order_by=self.order_by,
            limit=self.limit
        )

        # Generate random user agent header
        try:
            user_agent = UserAgent()
            headers = {'User-Agent': user_agent.random}
        except:
            print('Error occurred during fake user agent generation.')

        # Exception handling for connection errors
        try:
            req = requests.get(url, timeout=5, verify=True, headers=headers)
            req.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print('HTTP Error:', errh)
            sys.exit(0)
        except requests.exceptions.ConnectionError as errc:
            print('Error Connecting:', errc)
            sys.exit(0)
        except requests.exceptions.Timeout as errt:
            print('Timeout Error:', errt)
            sys.exit(0)
        except requests.exceptions.RequestException as err:
            print('There was an error.', err)
            sys.exit(0)

        # Exception handling for JSON decoding errors
        try:
            data = req.json()
        except json.decoder.JSONDecodeError:
            print('Could not decode JSON')


        # Adjust movie count according to starting page
        if self.page_arg == 1:
            movie_count = data.get('data').get('movie_count')
        else:
            movie_count = (data.get('data').get('movie_count')) - ((self.page_arg - 1) * self.limit)

        self.movie_count = movie_count
        self.url = url

    def __initialize_download(self):
        # Used for exit/continue prompt that's triggered after 10 existing files
        self.existing_file_counter = 0
        self.skip_exit_condition = False

        # YTS API sometimes returns duplicate objects and
        # the script tries to download the movie more than once.
        # IDs of downloaded movie is stored in this array
        # to check if it's been downloaded before
        self.downloaded_movie_ids = []

        if self.movie_count / self.limit <= 1:
            page_count = self.page_arg + 1                          # only one page, so range(1,2)
        else:
            page_count = int(self.movie_count/self.limit)               # more than one page
            if int(self.movie_count) % int(self.limit) > 0:           # add one page for excedent
                page_count = page_count + 1
            page_count = self.page_arg + page_count                     #range(1,pages+1)

        range_ = range(int(self.page_arg), page_count)


        if self.view == False:
            print('Initializing download with these parameters:\n')
            print('Directory:\t{}\nQuality:\t{}\nMovie Genre:\t{}\nMinimum Rating:\t{}\nCategorization:\t{}\nMinimum Year:\t{}\nStarting page:\t{}\nMovie posters:\t{}\nAppend IMDb ID:\t{}\nMultiprocess:\t{}\n'
                  .format(
                      self.directory,
                      self.quality,
                      self.genre,
                      self.minimum_rating,
                      self.categorize,
                      self.year_limit,
                      self.page_arg,
                      str(self.poster),
                      str(self.imdb_id),
                      str(self.multiprocess)
                      )
                 )

        text_desc = ""
        if self.movie_count <= 0:
            print('Could not find any movies with given parameters')
            sys.exit(0)
        else:
            print('Obtaining results...')
            #if self.view == False:
            #    if self.quality == "all":
            #        print('Found {} movies. Download starting...\n'.format(self.movie_count))
            #    else:
            #        print('Found {} torrents. Download starting...\n'.format(self.movie_count))
            #else:
            #    if self.quality == "all":
            #        print('Found {} movies.'.format(self.movie_count))
            #    else:
            #        print('Found {} torrents.'.format(self.movie_count))

        # Create progress bar
        if self.view == False:
            self.pbar = tqdm(
                total=self.movie_count,
                position=0,
                leave=True,
                desc='Downloading',
                unit='Files'
                )

        # Multiprocess executor
        # Setting max_workers to None makes executor utilize CPU number * 5 at most
        executor = ThreadPoolExecutor(max_workers=None)

        list_index = 0

        for page in range_:
            url = '{}{}'.format(self.url, str(page))

            # Generate random user agent header
            try:
                user_agent = UserAgent()
                headers = {'User-Agent': user_agent.random}
            except:
                print('Error occurred during fake user agent generation.')

            # Send request to API
            page_response = requests.get(url, timeout=5, verify=True, headers=headers).json()

            movies = page_response.get('data').get('movies')
            #print(movies)

            # Movies found on current page

            if self.multiprocess:
                # Wrap tqdm around executor to update pbar with every process
                tqdm(
                    executor.map(self.__filter_torrents, movies),
                    total=self.movie_count,
                    position=0,
                    leave=True
                    )

            else:
                index = 0
                while (index < len(movies)):
                    list_index = list_index + 1
                    movie = movies[index]
                    movie_torrent = self.__filter_torrents(movie,list_index)
                    if movie_torrent == None:
                        list_index = list_index - 1
                    self.__delete_duplicates(movies,movie_torrent)
                    index = index + 1
                    if self.quality == "all" and movie_torrent != None:         # still more torrents to solve
                        index = index - 1
                
        
        if list_index == 0:
            print('No movies match the specified parameters.')

        if self.view == False:
            self.pbar.close()
            print('Download finished.')


    def __delete_duplicates(self,movies,movie_torrent):
        for movie in movies:
            torrents = movie.get('torrents')
            for torrent in torrents:
                if torrent == movie_torrent:
                    torrents.remove(torrent)


    # Determine which .torrent files to download
    def __filter_torrents(self, movie, index):
        movie_id = str(movie.get('id'))
        movie_rating = movie.get('rating')
        movie_genres = movie.get('genres') if movie.get('genres') else ['None']
        movie_name_short = movie.get('title')
        imdb_id = movie.get('imdb_code')
        year = movie.get('year')
        language = movie.get('language')
        yts_url = movie.get('url')
        movie_torrents = movie.get('torrents')

        if len(movie_torrents) == 0:
            return None

        if self.quality != 'all':
            for movie_torrent in movie_torrents:
                if movie_torrent.get('quality') == self.quality:
                    break
        else:
            movie_torrent = movie_torrents[0]
        movie_quality = movie_torrent.get('quality')
        movie_size = movie_torrent.get('size')
        movie_type = movie_torrent.get('type').title()
        torrent_hash = movie_torrent.get('hash')

        if year < self.year_limit:
            return None

        # Every torrent option for current movie
        torrents = movie.get('torrents')
        # Remove illegal file/directory characters
        movie_name = movie.get('title_long').translate({ord(i):None for i in "'/\:*?<>|"})

        if self.view:
            print('#' + str(index) + ' ' + movie_name + ' (' + movie_type + ', ' + movie_quality +', ' + movie_size+') [' + torrent_hash + ']')

        # Used to multiple download messages for multi-folder categorization
        is_download_successful = False

        if movie_id in self.downloaded_movie_ids or self.view:
            return movie_torrent

        # In case movie has no available torrents
        if torrents is None:
            tqdm.write('Could not find any torrents for {}. Skipping...'.format(movie_name))
            return

        bin_content_img = (requests.get(movie.get('large_cover_image'))).content if self.poster else None

        # Iterate through available torrent files
        for torrent in torrents:
            quality = torrent.get('quality')
            movie_type = torrent.get('type')
            torrent_url = torrent.get('url')
            torrent_hash = torrent.get('hash')
            if self.categorize and self.categorize != 'rating':
                if self.quality == 'all' or self.quality == quality:
                    bin_content_tor = (requests.get(torrent.get('url'))).content

                    for genre in movie_genres:
                        path = self.__build_path(movie_name, movie_rating, quality, genre, imdb_id, torrent_hash, movie_type)
                        is_download_successful = self.__download_file(bin_content_tor, bin_content_img, path, movie_name, movie_id)
            else:
                if self.quality == 'all' or self.quality == quality:
                    self.__log_csv(movie_id, imdb_id, movie_name_short, year, language, movie_rating, quality, yts_url, torrent_url)
                    bin_content_tor = (requests.get(torrent_url)).content
                    path = self.__build_path(movie_name, movie_rating, quality, None, imdb_id, torrent_hash, movie_type)
                    is_download_successful = self.__download_file(bin_content_tor, bin_content_img, path, movie_name, movie_id)

            if is_download_successful and self.quality == 'all' or self.quality == quality:
                tqdm.write('Downloaded {} ({}, {}) [{}]'.format(movie_name, movie_type.title(), quality, torrent_hash))
                self.pbar.update()

        return movie_torrent

    # Creates a file path for each download
    def __build_path(self, movie_name, rating, quality, movie_genre, imdb_id, torrent_hash, movie_type):
        if self.csv_only:
            return

        directory = self.directory
        if self.categorize == 'none':
            pass
        if self.categorize == 'rating':
            directory += '/' + str(math.trunc(rating)) + '+'
        elif self.categorize == 'genre':
            directory += '/' + str(movie_genre)
        elif self.categorize == 'rating-genre':
            directory += '/' + str(math.trunc(rating)) + '+/' + movie_genre
        elif self.categorize == 'genre-rating':
            directory += '/' + str(movie_genre) + '/' + str(math.trunc(rating)) + '+'

        if self.poster:
            directory += '/' + movie_name

        os.makedirs(directory, exist_ok=True)

        if self.imdb_id:
            filename = '{} {} {} - {}'.format(movie_name, movie_type.title(), quality, imdb_id)
        else:
            filename = '{} {} {}'.format(movie_name, movie_type.title(), quality)

        filename = filename + ' (' + torrent_hash + ')'

        path = os.path.join(directory, filename)
        return path

    # Write binary content to .torrent file
    def __download_file(self, bin_content_tor, bin_content_img, path, movie_name, movie_id):
        if self.csv_only:
            return
        if self.view:
            return

        if self.existing_file_counter > 10 and not self.skip_exit_condition:
            self.__prompt_existing_files()

        if os.path.isfile(path):
            tqdm.write('{}: File already exists. Skipping...'.format(movie_name))
            print('file already exists')
            self.existing_file_counter += 1
            return False

        with open(path + '.torrent', 'wb') as torrent:
            torrent.write(bin_content_tor)
        if self.poster:
            with open(path + '.jpg', 'wb') as torrent:
                torrent.write(bin_content_img)

        self.downloaded_movie_ids.append(movie_id)
        self.existing_file_counter = 0
        return True

    def __log_csv(self, id, imdb_id, name, year, language, rating, quality, yts_url, torrent_url):
        path = os.path.join(os.path.curdir, 'YTS-Scraper.csv')
        csv_exists = os.path.isfile(path)

        with open(path, mode='a') as csv_file:
            headers = ['YTS ID', 'IMDb ID', 'Movie Title', 'Year', 'Language', 'Rating', 'Quality', 'YTS URL', 'IMDb URL', 'Torrent URL']
            writer = csv.DictWriter(csv_file, delimiter=',', lineterminator='\n', quotechar='"', quoting=csv.QUOTE_ALL, fieldnames=headers)

            if not csv_exists:
                writer.writeheader()

            writer.writerow({'YTS ID': id,
                             'IMDb ID': imdb_id,
                             'Movie Title': name,
                             'Year': year,
                             'Language': language,
                             'Rating': rating,
                             'Quality': quality,
                             'YTS URL': yts_url,
                             'IMDb URL': 'https://www.imdb.com/title/' + imdb_id,
                             'Torrent URL': torrent_url
                            })



    # Is triggered when the script hits 10 consecutive existing files
    def __prompt_existing_files(self):
        tqdm.write('Found 10 existing files in a row. Do you want to keep downloading? Y/N')
        exit_answer = input()

        if exit_answer.lower() == 'n':
            tqdm.write('Exiting...')
            sys.exit(0)
        elif exit_answer.lower() == 'y':
            tqdm.write('Continuing...')
            self.existing_file_counter = 0
            self.skip_exit_condition = True
        else:
            tqdm.write('Invalid input. Enter "Y" or "N".')

    def download(self):
        self.__get_api_data()
        self.__initialize_download()
