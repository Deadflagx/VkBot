from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api import VkUpload,vk_api
import traceback 
import requests
import json
from datetime import datetime   
import psycopg2
import time


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


group_id =          #VK Group ID
token = ''          #VK Group ADM token
vk = vk_api.VkApi(token=token) 
vk._auth_token()
vk.get_api()
upload = VkUpload(vk.get_api())
session = requests.Session()
longpoll = VkBotLongPoll(vk, group_id) 


keyboard = {
    "one_time": False,
    "buttons": [
        [{"action": {"type": "text","payload": "{\"button\": \"1\"}","label": "Shiba"},"color": "positive"},
         {"action": {"type": "text","payload": "{\"button\": \"1\"}","label": "Статистика лайков"},"color": "primary"},],
        [{"action": {"type": "text","payload": "{\"button\": \"1\"}","label": "Я в ТОПе"},"color": "secondary"},],
        [{"action": {"type": "text","payload": "{\"button\": \"1\"}","label": "Команды"},"color": "secondary"},]
    ]
}
keyboard = json.dumps(keyboard, ensure_ascii=False).encode('utf-8')
keyboard = str(keyboard.decode('utf-8'))



def send_message_plus_keyboard(text,keyboard):
    vk.method("messages.send", {"peer_id": event.object.peer_id, "message": text, "random_id": 0,"keyboard": keyboard})

def send_message(text):
    vk.method("messages.send", {"peer_id": event.object.peer_id, "message": text, "random_id": 0})

def send_msg_plus_attachment(text,attachment):
    vk.method("messages.send", {"user_id": event.object.peer_id,"message":'Источник: shibe.online', "attachment": attachments,"random_id": 0,'dont_parse_links':1})



while True:
    try:
        for event in longpoll.listen():
            if event.type == VkBotEventType.MESSAGE_NEW:
                if event.object.peer_id == event.object.from_id:
                    if event.object.text.lower() == 'начать':
                        """ Start """
                        msg = "Привет!\nНапишите ключевое слово 'Команды' если у Вас нет специальной клавиатуры на которой уже размещены все команды!"
                        send_message(msg,keyboard)
                    if event.object.text.lower() == 'shiba':
                        """ Send Shiba Image"""
                        attachments = [] 
                        image_url = 'http://shibe.online/api/shibes?count=1&urls=true'
                        response = requests.get(image_url)
                        image = session.get(response.json()[0], stream=True)
                        photo = upload.photo_messages(photos=image.raw)[0]
                        attachments.append('photo{}_{}'.format(photo['owner_id'], photo['id']))
                        msg = 'Источник: shibe.online'
                        send_msg_plus_attachment(msg,attachments)

                    if event.object.text.lower() == 'статистика лайков':
                        """ Like Statistic"""
                        cursor = connection.cursor()    
                        cursor.execute("SELECT totallikes, totalposts, nextrepeat, lastrepeat, currentdb FROM total")
                        fetchone = cursor.fetchone()
                        totallikes = fetchone[0]
                        totalposts = fetchone[1]
                        nextrepeat = fetchone[2]
                        lastrepeat = fetchone[3]
                        currentdb = fetchone[4]

                        cursor.execute("SELECT id, name ,likes FROM public." + currentdb + " ORDER BY likes DESC LIMIT 10")
                        DataTop = cursor.fetchall()  
                        cursor.close()
                        num = 1
                        text = ''

                        total_seconds_played =  round(time.time()) - nextrepeat
                        hours = total_seconds_played // 3600
                        total_seconds_played %= 3600
                        minutes = total_seconds_played // 60
                        total_seconds_played %= 60
                        seconds = total_seconds_played
                        total_time = ''
                        hword = 'Часа'
                        if hours == 1:
                            hword = 'Час'
                        TimeInit = [' {} {}'.format(hours,hword),' {} Мин.'.format(minutes),' {} Сек.'.format(seconds)]
                        for x in TimeInit:
                            spl = x.split(' ')
                            ispl = int(spl[1])
                            if ispl != 0:
                                total_time += x

                        stat = '\n\nВсего лайков: {}\nВсего поство: {}\nПримернное следующее обновление статистики: {}\nПоследний сбор статистики был: {}'.format(totallikes,totalposts, total_time ,datetime.fromtimestamp(lastrepeat))
                        for x in DataTop:
                            text += '№{} [{}{}|{}] {} ❤\n'.format(num,'id',x[0],x[1],x[2])
                            num += 1
                        msg = text + stat
                        send_message(msg)

                    if event.object.text.lower() == 'команды':
                        """ All commands"""
                        msg = ('Если у Вас нет специальной клавиаруры для быстроого использования команды, то ниже будут перечислены все команды!\n\nShiba - отправляет случайную фотографию Shiba-inu(не из группы).\nКоманды (это же ведь тоже команда xD) - отправит список всех актуальных команд.'+
                            '\nСтатистика лайков - покажет топ-10 участников группы по лайкам и так же другую статистическую информацию.\nЯ в ТОПе - эта команда сообщит о вашем текущем положении в топе, сколько лайков вы поставили и сколько нужно еще для повышения.'+
                            '\n\n*Все команды не чувствительный к регистру.')
                        send_message(msg)

                    if event.object.text.lower() == 'я в топе':
                        """ Get sender statistic relative of all users """
                        cursor = connection.cursor() 
                        cursor.execute("SELECT currentdb FROM total")
                        currentdb = cursor.fetchone()[0]
                        cursor.execute("SELECT * FROM public." +  currentdb + " WHERE id=%s", [event.object.from_id])
                        a = bool(cursor.rowcount)
                        if a == False:
                            msg = ("Хмм...\nНо Вас нет в базе лайкнувших :(\nВозможно Вы не ставил(а) лайки или обновление базы данных еще не было произведено, подожди немного.\n"+
                                  "Через сколько будет обновление можно узнать по команде 'Статистика лайков'")
                            send_message(msg)      
                        if a == True:
                            """ If user is in DataBase """
                            cursor.execute("SELECT row_number() OVER(ORDER BY likes DESC), id, name, likes FROM public."+currentdb)
                            allinfo = cursor.fetchall()
                            cursor.execute("SELECT COUNT(*) FROM "+currentdb)
                            countrows = cursor.fetchone()[0]
                            cursor.close()
                            for x, y in enumerate(allinfo):
                                if y[1] == event.object.from_id:
                                    index = x
                                    personinfo = y
                                    break
                            i = 1
                            if personinfo[0] == 1:
                                text = 'Вы находитесь на лидирующей позиции, чтобы Вас догнать участнику {} необходимо набрать еще {} лайков! Спасибо за вашу активность!'.format(allinfo[x+1][2], personinfo[3] - allinfo[x+1][3])
                            if personinfo[0] != 1:
                                while allinfo[x-i][3] <= personinfo[3]:
                                    i +=1 
                                needtonext = allinfo[x-i] 
                                text = '{}, Вы находитесь на {}/{} месте!\nВсего Ваших лайков: {}\nВам нужно еще {} лайков чтобы занять {} место :)'.format(personinfo[2], personinfo[0], countrows, personinfo[3], needtonext[3] - personinfo[3], needtonext[0])
                            send_message(text)
                            
    except (Exception) as e :
        print(traceback.format_exc()) 

