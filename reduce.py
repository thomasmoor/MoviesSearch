import glob
import gzip
import logging
import pandas as pd
import numpy as np
import requests
from time import time


ext='.tsv.gz'
www='https://datasets.imdbws.com'
names_list = [
  'name.basics',
  'title.principals',
  'title.ratings',
  'title.basics'
]

min_rating=4.0
min_votes=1000
no_threads=2

# Set up logging
logging.basicConfig(
  filename='reduce.log',
  # encoding='utf-8',
  format='%(asctime)s %(levelname)s:%(message)s',
  level=logging.DEBUG
)
logging.info("Logging activated")

dfrt=None
dfp=None
dfn=None

def display(df):
    # print(df.head())
    # print(df.info())
    print(df.describe())    
# display

def download_file(name):
    fn=name+ext
    url=www+'/'+name+ext
    print(f"download_file {url} to {fn}...")
    start = time()
    f = requests.get(url)
    open(fn, 'wb').write(f.content)
    print(f"Downloaded    {url} to {fn} in {time() - start}")
    logging.info(f"Downloaded {url} to {fn} in {time() - start}")
# download_file

def download_files():
    logging.debug("download_files")
    for name in names_list:
      download_file(name)
    # with ThreadPoolExecutor(max_workers = 3) as executor:
    #  executor.map(download_file, names)
# download_files
                     
def gunzip():
  for name in names_list:
    gunzip_file(name)
# gunzip

def gunzip_file(name):
    block_size=65536
    src=name+ext
    dst=name+".tsv"
    logging.info(f"Unzip {src} to {dst}...")
    print(f"Unzip {src} to {dst}...")
    with gzip.open(src, 'rb') as sf, open(dst, 'wb') as df:
      while True:
        block = sf.read(block_size)
        if not block:
          break
        else:
          df.write(block)
# gunzip_file

def names():
    global dfn
    print("Load and filter Names...")
    
    # the list of names nconsts
    print(f"Principals        : {dfp.shape}")
    l=dfp['nconst'].unique().tolist()
    
    # Load the names
    dfn = pd.read_csv("name.basics.tsv",sep="\t",low_memory=False)
    print(f"Names             : {dfn.shape}")
    
    # Filter the names
    dfn=dfn[dfn.nconst.isin(l)]
    print(f"Names             : {dfn.shape}")
    
    # Save the data
    dfn.to_csv("names.min")
    
    # Save the directors
    df=dfn.dropna(subset=['primaryProfession'])
    df=df[df.primaryProfession.str.contains("director")]
    print(f"Directors         : {df.shape}")
    df.to_csv("directors.min")

# names

def principals():
    global dfrt
    global dfp
    print("Load and filter Principals...")
    # Get the list of titles tconsts
    print(f"Rating_Titles          : {dfrt.shape}")
    l=dfrt['tconst'].tolist()
    # print(f"List of Titles         : {l.ndim}")
    
    # Load all the principals
    dfp = pd.read_csv("title.principals.tsv",sep="\t",low_memory=False)
    print(f"Principals             : {dfp.shape}")
    
    # Filter the principals
    dfp=dfp[dfp.tconst.isin(l)]
    print(f"Principals after filter: {dfp.shape}")
    
    # Backup the data
    dfp.to_csv("principals.min")

# principals

def ratings_titles():
    global dfrt
    
    print("Merge and filter Ratings and Titles...")

    # Load the titles
    dft = pd.read_csv("title.basics.tsv",sep="\t",low_memory=False)
    print(f"Titles           : {dft.shape}")
    
    # print("Test Existence")
    # tmp=dft[dft['originalTitle'].str.contains('furious', na=False,case=False)]
    # print(tmp.originalTitle)

    # Load the ratings
    dfr=pd.read_csv("title.ratings.tsv",sep="\t",low_memory=False)
    print(f"Ratings          : {dfr.shape}")
    
    # Merge the titles and the ratings
    dfrt = pd.merge(dfr,dft,on="tconst",how="right")
    print(f"Ratings_Titles   : {dfrt.shape}")

    # print("Unique isAdult")    
    # print(dfrt.isAdult.unique())
    
    dfrt = dfrt[dfrt.numVotes >= min_votes]
    print(f"RT Votes >={min_votes}  : {dfrt.shape}")
    dfrt = dfrt[dfrt.averageRating  >= min_rating]
    print(f"RT Rating>={min_rating}   : {dfrt.shape}")
    dfrt = dfrt[dfrt.isAdult != '1']
    print(f"Not Adult        : {dfrt.shape}")
    dfrt = dfrt[(dfrt.titleType == "movie") | (dfrt.titleType == "tvMovie")]
    print(f"Movie or TV Movie: {dfrt.shape}")
    dfrt.drop(["isAdult","endYear"],axis=1,inplace=True)
    ## df.dropna() # subset=['averageRating'])
    ## df.dropna() # subset=['primaryTitle'])
    ## df = df[df.startYear.notnull()]    
    ## print("ratings_titles:")
    # display(dfrt)
    dfrt.to_csv("ratings_titles.min")
# ratings_titles

def reduce():
    global dfrt
    global dfp
    global dfn
        
    # Ratings and Titles merged and filtered
    ratings_titles()
    
    # Filter the principals data
    # dfrt=pd.read_csv("ratings_titles.min")
    principals()
    
    # Names
    dfp=pd.read_csv("principals.min")
    names()
    
# reduce

if __name__ == '__main__':

    # Download the files
    download_files()

    # Unzip the files
    gunzip()
    
    # Reduce the amount of data
    reduce()
    
