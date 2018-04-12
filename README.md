# Douyu_danmu

'''
利用斗鱼弹幕 api
尝试抓取斗鱼tv指定房间的弹幕
'''
# -*- coding:utf-8 -*-

import multiprocessing
import socket
import time
import xlwt
import re
import requests
import signal
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time

Author = "Sherwin"

# 构造socket连接，和斗鱼api服务器相连接
client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = socket.gethostbyname("openbarrage.douyutv.com")
port = 8601
client.connect((host, port))



# 初始化数据库

# df = pd.DataFrame(0, columns = list(''))

# 弹幕查询正则表达式
uid_path = re.compile(b'uid@=(.+?)/nn@')
nickname_path = re.compile(b'nn@=(.+?)/txt@')
level_path = re.compile(b'level@=([1-9][0-9]?)/sahf@')
danmu_path = re.compile(b'txt@=(.+?)/cid@')


def sendmsg(msgstr):
    '''构造并发送符合斗鱼api的请求'''

    msg = msgstr.encode('utf-8')
    data_length = len(msg) + 8
    code = 689
    # 构造协议头
    msgHead = int.to_bytes(data_length, 4, 'little') \
        + int.to_bytes(data_length, 4, 'little') + \
        int.to_bytes(code, 4, 'little')
    client.send(msgHead)
    sent = 0
    while sent < len(msg):
        tn = client.send(msg[sent:])
        sent = sent + tn


def DM_start(roomid, ws, wb):
    # 构造登录授权请求
    msg = 'type@=loginreq/roomid@={}/\0'.format(roomid)
    sendmsg(msg)
    # 构造获取弹幕消息请求
    msg_more = 'type@=joingroup/rid@={}/gid@=-9999/\0'.format(roomid)
    sendmsg(msg_more)
    product_2 = {'UID':'','Text':''}
    # df_all = pd.DataFrame([])
    result = []
    index = 1
    print('------------------欢迎链接到{}的直播间------------------'.format(get_name(roomid)))
    while True:
        # 服务端返回的数据
        data = client.recv(1024)
        # 通过re模块找发送弹幕的用户名和内容
        uid_more = uid_path.findall(data)
        nickname_more = nickname_path.findall(data)
        level_more = level_path.findall(data)
        danmu_more = danmu_path.findall(data)
        unixt_more = str(int(time.time()))
        
        if not level_more:
            level_more = b'0'
        if not data:
            break
        
        else:
            for i in range(0, len(danmu_more)):
                try:
                    # 输出信息
                    product_1 = {
                        'No' : index,
                        'UID': uid_more[0].decode('utf8'),
                        'nickname' : nickname_more[0].decode('utf8'),
                        'Level' : level_more[0].decode('utf8'),
                        'unix_stamp' : unixt_more,
                        'Text' : danmu_more[0].decode('utf8')
                    }
                    result_add = [product_1['No'], \
                        product_1['UID'], \
                        product_1['nickname'], \
                        product_1['Level'], \
                        product_1['unix_stamp'],\
                        product_1['Text'], \
                    ]
                    if (product_1['UID'] == product_2['UID']) and (product_1['Text'] == product_2['Text']):
                        break
                    else:
                        product_2 = product_1
                        print(product_1)
                        # df_add = pd.DataFrame(product_1, index = roomid)
                        # df_add = df_add.sort_index(axis = 1, ascending = False)
                        # df_all.append(df_add)
                        result.append(result_add)
                        excel_write(index, result, ws, wb)
                        index += 1
                except Exception as e:
                    continue

    

# 没问题
def keeplive():
    '''
    保持心跳，15秒心跳请求一次
     '''
    while True:
        msg = 'mrkl/\0'
        sendmsg(msg)
        time.sleep(15)
        # print('心跳一下')



# 没问题
def get_name(roomid):
    r = requests.get("http://www.douyu.com/" + roomid)
    soup = BeautifulSoup(r.text, 'lxml')
    


    return soup.find('a',{'class', 'zb-name'}).string

# 没问题
def logout():
    '''
    与斗鱼服务器断开连接
    关闭线程
    '''
    msg = 'type@=logout/'
    sendmsg(msg)
    print('已经退出服务器')

# 没问题
def signal_handler(signal, frame):
    '''
    捕捉 ctrl+c的信号 即 signal.SIGINT
    触发hander：
    登出斗鱼服务器
    关闭进程
    '''

    p1.terminate()
    p2.terminate()
    logout()
    print('Bye')

def excel_write(result, index, ws, wb):
    #写入excel
    ws.write(index, 0, result[0])
    ws.write(index, 1, result[1])
    ws.write(index, 2, result[2])
    ws.write(index, 3, result[3])
    ws.write(index, 4, result[4])
    ws.write(index, 5, result[5])
    wb.save('danmu.xlsx')

    # df_all.to_csv("{}.csv".format(roomid), ignore_index = True, sep = ',')

if __name__ == '__main__':
    
    # 交互式输入房间ID
    room_id = input('请输入房间ID(数字)：  ')
    #创建excel表格用于收集信息
    wb = xlwt.Workbook()
    ws = wb.add_sheet('{}'.format(room_id))
    ws.write(0,0,"index")
    ws.write(0,1,u'UID')
    ws.write(0,2,u'nickname')
    ws.write(0,3,u'Level')
    ws.write(0,4,u'unix_stamp')
    ws.write(0,5,u'Text')

    # 开启signal捕捉
    signal.signal(signal.SIGINT, signal_handler)

    # 开启弹幕和心跳进程
    p1 = multiprocessing.Process(target=DM_start, args=(room_id,ws,wb)) #这个逗号不能删
    p2 = multiprocessing.Process(target=keeplive)
    p1.start()
    p2.start()
