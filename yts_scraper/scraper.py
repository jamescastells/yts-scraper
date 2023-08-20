from operator import truediv
import os
import sys
import math
import json
import csv
import requests
from tqdm import tqdm
from fake_useragent import UserAgent
from multiprocessing.dummy import Pool as ThreadPool
from tabulate import tabulate

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
        self.format = args.format

        self.url = None
        self.existing_file_counter = None
        self.skip_exit_condition = None
        self.pbar = None
        self.torrent_count = 0
        self.movies = []
        self.torrentNumber = 1
        self.numberOfPages = 0
        self.hasObtainednumberOfPages = False
        self.table = [["#","Name","Year","Format","Quality","Size","Hash"]]

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

    def __initialize_download(self):
        # Used for exit/continue prompt that's triggered after 10 existing files
        self.existing_file_counter = 0
        self.skip_exit_condition = False

        if self.view == False and self.csv_only == False:
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

        if self.torrent_count <= 0:
            print('Could not find any movies with given parameters')
            sys.exit(0)
        else:
            if self.view == True:
                print('Displaying results...')
            if self.view == False and self.csv_only == False:
                print('Download starting...\n')

        # Create progress bar
        if self.view == False and self.csv_only == False:

            self.pbar = tqdm(
                total=self.torrent_count,
                position=0,
                leave=True,
                desc='Downloading',
                unit='Files'
                )

        movies = self.movies

        if self.multiprocess:
            pool = ThreadPool(10)
            pool.map(self.__downloadMovie, movies)
        else:
            for movie in movies:
                self.__downloadMovie(movie)

        if self.view:
            print(tabulate(self.table, headers='firstrow', tablefmt='fancy_grid'))           

        if self.view == False and self.csv_only == False:
            self.pbar.close()
            print('Download finished.')
    
    def __downloadMovie(self,movie):
        movie_id = str(movie.get('id'))
        movie_rating = movie.get('rating')
        movie_genres = movie.get('genres') if movie.get('genres') else ['None']
        movie_name_short = movie.get('title')
        imdb_id = movie.get('imdb_code')
        year = movie.get('year')
        language = movie.get('language')
        yts_url = movie.get('url')
        movie_name = movie.get('title_long').translate({ord(i):None for i in "'/\:*?<>|"})
        movie_torrents = movie.get('torrents')
        for movie_torrent in movie_torrents:
            movie_quality = movie_torrent.get('quality')
            movie_size = movie_torrent.get('size')
            movie_type = movie_torrent.get('type').title()
            torrent_hash = movie_torrent.get('hash')
            torrent_url = movie_torrent.get('url')
            if self.view:
                #print('#' + str(self.torrentNumber) + ' ' + movie_name + ' (' + movie_type + ', ' + movie_quality +', ' + movie_size+') [' + torrent_hash + ']')
                self.table.append([str(self.torrentNumber),movie_name_short,year,movie_type,movie_quality,movie_size,torrent_hash])
            if self.csv_only:
                self.__log_csv(movie_id, imdb_id, movie_name_short, year, language, movie_rating, movie_quality, yts_url, torrent_url, movie_type)
            if self.view == False and self.csv_only == False:
                bin_content_img = (requests.get(movie.get('large_cover_image'))).content if self.poster else None
                bin_content_tor = (requests.get(torrent_url)).content
                is_download_successful = False
                if self.categorize == "genre" or self.categorize == "rating-genre" or self.categorize == "genre-rating":
                    for genre in movie_genres:
                        path = self.__build_path(movie_name, movie_rating, movie_quality, genre, imdb_id, torrent_hash, movie_type)
                        is_download_successful = self.__download_file(bin_content_tor, bin_content_img, path, movie_name, movie_id) 
                else:
                    path = self.__build_path(movie_name, movie_rating, movie_quality, None, imdb_id, torrent_hash, movie_type)
                    is_download_successful = self.__download_file(bin_content_tor, bin_content_img, path, movie_name, movie_id)
                if is_download_successful:
                    tqdm.write('Downloaded {} ({}, {}) [{}]'.format(movie_name, movie_type.title(), movie_quality, torrent_hash))
                    self.pbar.update()
            self.torrentNumber += 1

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

        self.existing_file_counter = 0
        return True

    def __log_csv(self, id, imdb_id, name, year, language, rating, quality, yts_url, torrent_url, type):
        print("Saved movie {} in csv...".format(name))
        path = os.path.join(os.path.curdir, 'YTS-Scraper.csv')
        csv_exists = os.path.isfile(path)

        with open(path, mode='a') as csv_file:
            headers = ['YTS ID', 'IMDb ID', 'Movie Title', 'Year', 'Language', 'Rating', 'Quality', 'Format', 'YTS URL', 'IMDb URL', 'Torrent URL']
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
                             'Format': type,
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
        self.__filterMoviesAndObtainTorrents()
        self.__initialize_download()

    def __filterMoviesAndObtainTorrents(self):

        print('Obtaining torrents...')

        self.url = '''https://yts.mx/api/v2/list_movies.json?genre={genre}&minimum_rating={minimum_rating}&sort_by={sort_by}&query_term={text}&order_by={order_by}&limit={limit}&page='''.format(
            genre=self.genre,
            minimum_rating=self.minimum_rating,
            sort_by=self.sort_by,
            text=self.text,
            order_by=self.order_by,
            limit=self.limit
        )

        numberOfTorrents = 0
        i = self.page_arg

        while(self.hasObtainednumberOfPages == False or i <= self.numberOfPages):
            url = '{}{}'.format(self.url, str(i))
            # Generate random user agent header
            try:
                user_agent = UserAgent()
                headers = {'User-Agent': user_agent.random}
            except:
                print('Error occurred during fake user agent generation.')
            try:
                page_response = requests.get(url, timeout=5, verify=True, headers=headers).json()
            except:
                print('There was an error connecting to yts. Try again.')
                return
            movies = page_response.get('data').get('movies')
            if (self.hasObtainednumberOfPages == False):               # let's set the limit
                movie_count = int(page_response.get('data').get('movie_count'))
                pages_count = int(movie_count / self.limit)
                if (movie_count % self.limit > 0):
                    pages_count = pages_count + 1
                self.numberOfPages = pages_count
                self.hasObtainednumberOfPages = True
            if movies == None:
                return
            j = 0
            while(j<len(movies)):
                movie = movies[j]
                if (movie.get('year')<self.year_limit):
                    movies.remove(movie)
                    j = j - 1
                if self.format != "all" and movie in movies:                    # remove all that's not the quality specified
                    torrents = movie.get('torrents')
                    x = 0
                    while (x < len(torrents)):
                        torrent = torrents[x]
                        if (torrent.get('type') != self.format):
                            torrents.remove(torrent)
                            x = x - 1
                        x = x + 1
                    if len(torrents) == 0:
                        movies.remove(movie)
                        j = j - 1
                if self.quality != "all" and movie in movies:       # remove all that's not the format specified  
                    torrents = movie.get('torrents')
                    x = 0
                    while (x < len(torrents)):
                        torrent = torrents[x]
                        if (torrent.get('quality') != self.quality):
                            torrents.remove(torrent)
                            x = x - 1
                        x = x + 1
                    if len(torrents) == 0:
                        movies.remove(movie)
                        j = j - 1
                j = j +1
            for movie in movies:
                numberOfTorrents = numberOfTorrents + len(movie.get('torrents'))
            self.movies = self.movies + movies
            if (i < self.numberOfPages):
                print('Obtained {} torrents so far... ({}/{})'.format(str(numberOfTorrents),str(i),str(self.numberOfPages)))
            else:
                print('Obtained {} torrents. ({}/{})'.format(str(numberOfTorrents),str(i),str(self.numberOfPages)))
            i = i + 1
        self.torrent_count = numberOfTorrents

