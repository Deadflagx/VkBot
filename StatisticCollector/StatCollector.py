import psycopg2
import vk_api
from datetime import datetime
import time
import traceback


try:
    connection = psycopg2.connect(user = "user",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "database")
    cursor = connection.cursor()
    print ( connection.get_dsn_parameters(),"\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
cursor.close()


token = ''          #VK Group ADM token
vk = vk_api.VkApi(token=token) 
tools = vk_api.VkTools(vk)
vk._auth_token()
vk = vk.get_api()


def clear_database(curdb):
  cursor = connection.cursor()
  cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
  tables = cursor.fetchall()
  for x in tables:
    if x[0] != curdb:
      """ Truncate all tables except current DB with data"""
      cursor.execute("TRUNCATE TABLE public."+x[0])
      connection.commit()


owner_id = #Group ID
TIMEOUT = 14400 # Time to sleep(in seconds)
currentdb = None

clear_database(1)

while  True:
  try:
    dt_a = time.time()
    """ DataBase Init"""
    if currentdb == 'likes':
      currentdb = 'likes2'
    elif currentdb == 'likes2':
      currentdb = 'likes'
    elif currentdb == None:
      currentdb = 'likes'

    """ All posts getter"""
    posts = tools.get_all_iter('wall.get', 50, {'owner_id': owner_id})
    totalposts = 0
    totallikes = 0

    for x in posts:
      """Get id, names and count likes from every post"""
      totalposts += 1
      curlikes = x['likes']['count']
      totallikes += x['likes']['count']
      if curlikes > 0:
        t = vk.likes.getList(type='post',owner_id=owner_id,item_id=x['id'],filter='likes',extended=1)
        for y in t['items']:
          idd = y['id']
          name = y['first_name'] + ' ' + y['last_name']
          cursor = connection.cursor()
          cursor.execute("SELECT * FROM public.likes WHERE id=%s", [y['id']])
          a = bool(cursor.rowcount)
          if a == False:
            """ If user isnt in DataBase"""
            """ Add new id, name in DB"""
            cursor.execute("INSERT INTO public.likes (id,name,likes) VALUES (%s,%s,%s)" , (idd,name,1))            
            connection.commit()
          if a == True:
            """ If user already in DB"""
            """ Just add +1 like"""
            cursor.execute("UPDATE public.likes SET likes = likes + 1 WHERE id=%s", [y['id']])
            connection.commit() 
  except:
    print(traceback.format_exc())


  dt_b = time.time()
  nextrepeat = TIMEOUT - (round((dt_b - dt_a)//10) + round((dt_b - dt_a)))

  clear_database(currentdb)
  cursor.execute("INSERT INTO public.total (totallikes, totalposts ,nextrepeat, lastrepeat, currentdb) VALUES (%s,%s,%s,%s,%s)", (totallikes, totalposts, nextrepeat+round(time.time()), round(time.time()), currentdb))
  connection.commit()
  cursor.close()

  time.sleep(nextrepeat)
    
