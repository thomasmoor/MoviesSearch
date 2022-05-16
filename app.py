from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin   # pip install -U flask-cors
import json
import logging
import numpy as np
import pandas as pd

# https://www.imdb.com/interfaces/
# https://medium.com/analytics-vidhya/exploratory-data-analysis-imdb-dataset-cff0c3991ad5

min_rating=5.0
min_votes=1000

df_ratings_titles=None
df_principals=None
df_names=None

#
# Logging
# https://flask.palletsprojects.com/en/2.0.x/logging/
#
logging.basicConfig(
  filename='tmdb.log', 
  # encoding='utf-8', 
  format='%(asctime)s %(levelname)s:%(message)s', 
  level=logging.DEBUG
)

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
    
params=""

@app.route('/', methods=['GET','POST'])
@cross_origin()
def slash():
    print(f"slash - got request: {request.method}")
    if request.method == 'GET':
        print(f"GET - Params: {params}")
        api_urls={
            'list': '[GET]  /',
            'run':  '[POST] /',
        }
        # jsonify accepts a dict or a list
        return jsonify(api_urls)

    elif request.method == 'POST':
        print(f"POST - params: {params}")
        # data = json.loads(request.data)
        # print("Data:")
        # print(data)
        resp={'message': "Processed"}
        return jsonify(resp)
    else:
        print("Post error 405 - Not allowed")
        resp={'Method not allowed': request.method}
        return jsonify(resp)
# slash()

@app.route('/find_name', methods=['POST'])
@cross_origin()
def find_name():
    print(f"find_name - got request: {request.method}")
    if request.method == 'POST':
        logging.debug(f"params: {params}")
        logging.debug("request:")
        logging.debug(request)
        logging.debug("request.data:")
        logging.debug(request.data)
        search=json.loads(request.data)
        logging.debug("search: "+search)
        # search = json.loads(request.data)
        # search="Marie";
        logging.debug(f"Search: {search} - DFs: Names: {df_names.size} Principals: {df_principals} Movies: {df_ratings_titles}")
        df=search_name(search,10)
        js=df.to_json(orient='records')
        logging.debug(f"json:{js}")
        resp={'message': "Processed"}
        return js
        # return jsonify(resp)
    else:
        logging.debug("Post error 405 - Not allowed")
        resp={'Method not allowed': request.method}
        return jsonify(resp)
# find_name()

@app.route('/movies', methods=['POST'])
@cross_origin()
def movies():
    print(f"movies - got request: {request.method}")
    if request.method == 'POST':
        logging.debug(f"params: {params}")
        logging.debug("request:")
        logging.debug(request)
        logging.debug("request.data:")
        logging.debug(request.data)
        d=json.loads(request.data)
        logging.debug("d:")
        logging.debug(d)
        logging.debug(d['name1'])
        
        genre=d['genre']
        nconst1=d['name1']
        nconst2=d['name2']
        if d['rating'].isdigit():
          rating=float(d['rating'])
        else:
          rating=0
        title='title'
        if d['votes'].isdigit():
          votes=int(d['votes'])
        else:
          votes=0
        if d['yearEnd'].isdigit():
          year_end=int(d['yearEnd'])
        else:
          year_end=9000
        if d['yearStart'].isdigit():
          year_start=int(d['yearStart'])
        else:
          year_start=0

        df=find_titles(
            genre,
            nconst1,
            nconst2,
            rating,
            title,
            votes,
            year_end,
            year_start
        )
        logging.debug("To json")
        js=df.to_json(orient='records')
        logging.debug("json")
        # logging.debug(js)
        # resp={'message': "Processed"}
        return js
        # return jsonify(resp)
    else:
        logging.debug("Post error 405 - Not allowed")
        resp={'Method not allowed': request.method}
        return jsonify(resp)
# movies

def display(df):
    # print(df.head())
    print(df.info())
    print(df.describe())    
# display

def search_name(search,max_rows):
  logging.debug(f"search_name search: {search}")
  # display(df_names)
  df=df_names[df_names.primaryName.str.contains(search,na=False, case=False)]
  df['display']=df.primaryName+' ('+df.birthYear+')'
  # display(df)
  print(df)
  logging.debug(f"search_name df")
  return df.head(max_rows)
# search_name

def find_nconst():
    df_title_basics = pd.read_csv(open(file,"rb"))
    use = df_title_basics[df_title_basics.runtimeMinutes.notnull()]
    use["runtimeMinutes"] = use.runtimeMinutes.astype(int)
    print(use[use.runtimeMinutes>50000])
# Find nconst

def find_titles(genre,nconst1,nconst2,rating,title,votes,year_end,year_start):
    # Starting point cases
    # - No movie for name1 or Name2
    # - No movie for name1
    # - No movie for name2
    # - Movie(s) for name1 and name2
    
    logging.debug(f"find_titles: g:{genre} n1:{nconst1} n2:{nconst2} r:{rating} t:{title} v:{votes} ys:{year_start} ye:{year_end}")
    filtered=""
    df=df_ratings_titles
    logging.debug(f"find_titles - titles         : {df.shape}")
    if nconst1!="":
        df1=df_principals[df_principals.nconst==nconst1]
        logging.debug(f"find_titles - principals n1={nconst1}  : {df1.shape}")
        l1=df1['tconst'].unique().tolist()
        df=df[df.tconst.isin(l1)]
        logging.debug(f"find_titles - titles     n1={nconst1}  : {df.shape}")
    if nconst2!="":
        df2=df_principals[df_principals.nconst==nconst2]
        logging.debug(f"find_titles - principals n2={nconst2}  : {df2.shape}")
        l2=df2['tconst'].unique().tolist()
        df=df[df.tconst.isin(l2)]
        logging.debug(f"find_titles - titles     n2={nconst2}  : {df.shape}")
    logging.debug(f"find_titles - titles         : {df.shape}")
    
    # numVotes
    # titleType
    # primaryTitle
    # originalTitle
    # startYear
    # runtimeMinutes
    # genres
    if genre!="":
        logging.debug(f"find_titles filter genre: {genre}")
        filtered+=" g:"+genre
        df=df.dropna(subset=['genres'])
        df=df[df.genres.str.contains(genre)]
    if rating > 0:
        filtered+=" r:"+str(rating)
        df=df[pd.to_numeric(df['averageRating'], errors='coerce').notnull()]
        df=df[df.averageRating > rating]
    # if title!="":
    #    filtered+=" t:"+title
    #    df=df.dropna(subset=['originalTitle','primaryTitle'])
    #    df=df[df.originalTitle.str.contains(title) or df.primaryTitle.str.contains(title)]
    if votes > 0:
        filtered+=" v:"+str(votes)
        df=df[pd.to_numeric(df['numVotes'], errors='coerce').notnull()]
        df=df[df.numVotes > votes]
    if year_end > 0:
        filtered+=" ye:"+str(year_end)
        df=df[pd.to_numeric(df['startYear'], errors='coerce').notnull()]
        df=df[df.startYear.astype(float).astype('Int64', errors='ignore') <= year_end]
    if year_start > 0:
        filtered+=" ys:"+str(year_start)
        df=df[pd.to_numeric(df['startYear'], errors='coerce').notnull()]
        df=df[df.startYear.astype(float).astype('Int64', errors='ignore') >= year_start]
    print(f"Filterd: {filtered}")

    print("Movies")
    display(df)
    print(df)
    
    # Casts
    l=df['tconst'].unique().tolist()
    df_casts=df_principals[df_principals['tconst'].isin(l)]
    print("Casts:")
    print(df_casts)
    df_casts_names = pd.merge(df_casts,df_names,on="nconst",how="left")
    print("Casts_names - joined:")
    print(df_casts_names)
    
    # Keep for director or searched actor
    df_casts_names=df_casts_names.dropna(subset=['category'])
    df_casts_names=df_casts_names[df_casts_names.category=='director']
    # df_casts=df_casts[df_casts.category=='director' or (nconst1!="" and df_casts.primaryName==nConst1) or (nconst2!="" and df_casts.primaryName==nConst2)]

    df_movies_casts_names = pd.merge(df_casts_names,df,on="tconst",how="left")
    print("Movies Casts_names - joined:")
    print(df_movies_casts_names)
    
    return df_movies_casts_names.head(30)
# Find titles

def init():
    print("init()")
    logging.info('init()')
    names()
    principals()
    ratings_titles()
    global params
    params=f"init(): Names: {df_names.size} Principals: {df_principals} Titles: {df_ratings_titles}"
    logging.info(params)
# init

def list_genres():
    l=[]
    print("list_genres")
    g=df_ratings_titles['genres'].unique().tolist()
    for genres in g:
      if isinstance(genres, str):
        tokens=genres.split(',')
        for t in tokens:
          # print(t)
          if not t in l:
            l.append(t)
      # print(genres)
    return l
# list_genres

def list_names():
    print("list_names:")
    display(df_names)
    # print(df_names.to_json(orient='table'))
# list_names

def names():
    global df_names
    print("Load Names...")
    df_names=pd.read_csv("names.min")
    # display(df_names)
# names

def principals():
    global df_principals
    print("Load Principals...")
    df_principals=pd.read_csv("principals.min")
    # display(df_principals)
# principals

def ratings_titles():
    global df_ratings_titles
    print("Load Ratings and Titles..")
    file="ratings_titles.min"
    df_ratings_titles = pd.read_csv(file)
    # display(df_ratings_titles)
# ratings_titles

init()
    
if __name__ == '__main__':

    # l=list_genres()
    # with open("genres.txt", "w") as f:
    #   for g in l:
    #     f.write("   { value: \""+g+"\" }, "+"\n")
    # print(l)

    # List the available names
    # list_names()
    
    # Find Titles
    genre=""
    name1="" # "nm0001191"
    name2="" # "nm0001705"
    rating=5.5
    title=""
    votes=1000
    year_start=2021
    year_end=2022
    # df = find_titles(genre,name1,name2,rating,title,votes,year_end,year_start)
    # df.to_csv("movies_casts_names.csv")

    # Find name
    # search="Marie-Josée Croze"
    # search="Marie"
    # df = find_name(search,10)
    # df.to_csv("find_name.csv")
    
    # To open port:
    # Type "Windows Firewall" in search
    # Go to Windows Defender Firewall
    # > Advanced Settings > Inbound rules
    # > New Rule > Port > Next
    app.run(debug=True,host='0.0.0.0', port=5102)
