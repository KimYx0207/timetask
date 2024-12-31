#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os
from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.styles import NamedStyle
import hashlib
import base64
import arrow
import re
from typing import List
import time
from datetime import datetime, timedelta
from lib import itchat
from lib.itchat.content import *
from channel.chat_message import ChatMessage
from croniter import croniter
import threading
import logging

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from channel.wechatnt.ntchat_channel import wechatnt
except Exception as e:
    print(f"未安装ntchat: {e}")

try:
    from channel.wework.run import wework
except Exception as e:
    print(f"未安装wework: {e}")

class ExcelTool(object):
    __file_name = "timeTask.xlsx"
    __sheet_name = "定时任务"
    __history_sheet_name = "历史任务"
    __dir_name = "taskFile"
    
    # 新建工作簿
    def create_excel(self, file_name: str = __file_name, sheet_name=__sheet_name, history_sheet_name=__history_sheet_name):
        # 文件路径
        workbook_file_path = self.get_file_path(file_name)

        # 创建Excel
        if not os.path.exists(workbook_file_path):
            wb = Workbook()
            column_list_first = ['A', 'B', 'C', 'D', 'L']
            width_value_first = 20
            column_list_two = ['E', 'F', 'H', 'J']
            width_value_two = 40
            column_list_three = ['G', 'I', 'K']
            width_value_three = 70
            width_value_four = 600
            
            # 设置日期格式
            date_format = NamedStyle(name='date_format')
            date_format.number_format = 'YYYY-MM-DD'

            #sheet1
            ws = wb.create_sheet(sheet_name, 0)
            # 类型处理
            for column in ws.columns:
                #日期格式
                if column == "D":
                    for cell in column:
                        cell.style = date_format
                #字符串        
                else:
                    for cell in column:
                        cell.number_format = '@'
            
            #宽度处理 
            for column in column_list_first:
                ws.column_dimensions[column].width = width_value_first
            for column in column_list_two:
                ws.column_dimensions[column].width = width_value_two
            for column in column_list_three:
                ws.column_dimensions[column].width = width_value_three
            ws.column_dimensions["M"].width = width_value_four 
              
            #sheet2
            ws1 = wb.create_sheet(history_sheet_name, 1)
            # 类型处理 - 设置为字符串
            for column in ws1.columns:
                for cell in column:
                    cell.number_format = '@'
                    
            #宽度处理        
            for column in column_list_first:
                ws1.column_dimensions[column].width = width_value_first
            for column in column_list_two:
                ws1.column_dimensions[column].width = width_value_two
            ws1.column_dimensions["M"].width = width_value_three     
                    
            wb.save(workbook_file_path)
            print("定时Excel创建成功，文件路径为：{}".format(workbook_file_path))
            
        else:
            wb = load_workbook(workbook_file_path)
            if not history_sheet_name in wb.sheetnames:
                wb.create_sheet(history_sheet_name, 1)
                wb.save(workbook_file_path)
                print(f"创建sheet: {history_sheet_name}")
            else:
                print("timeTask文件已存在, 无需创建")
                

    # 读取内容,返回元组列表
    def readExcel(self, file_name=__file_name, sheet_name=__sheet_name):
        # 文件路径
        workbook_file_path = self.get_file_path(file_name)
        
        # 文件存在
        if os.path.exists(workbook_file_path):
            wb = load_workbook(workbook_file_path)
            ws = wb[sheet_name]
            data = list(ws.values)
            if data is None or len(data) == 0:
                print("[timeTask] 数据库timeTask任务列表数据为空")
            else:
                # 使用 logging 直接记录，不依赖 self.debug
                for row in data:
                    logging.debug(f"读取任务数据: {row}")
            return data
        else:
            print("timeTask文件不存在, 读取数据为空")
            self.create_excel()
            return []

    # 将历史任务迁移指历史Sheet
    def moveTasksToHistoryExcel(self, tasks, file_name=__file_name, sheet_name=__sheet_name, history_sheet_name=__history_sheet_name):
        # 文件路径
        workbook_file_path = self.get_file_path(file_name)
        
        # 文件存在
        if os.path.exists(workbook_file_path):
            wb = load_workbook(workbook_file_path)
            ws = wb[sheet_name]
            data = list(ws.values)
            
            #需要删除的坐标
            rows_to_delete = []
            #遍历任务列表
            for i, item in enumerate(data):
                 #任务ID
                 taskId = item[0]
                 for _, hisItem in enumerate(tasks):
                    #历史任务ID
                    his_taskId = hisItem[0]
                    if taskId == his_taskId:
                        rows_to_delete.append(i + 1)
            
            #排序坐标
            sorted_rows_to_delete = sorted(rows_to_delete, reverse=True)
                        
            #遍历任务列表
            for dx in sorted_rows_to_delete:
                #移除
                ws.delete_rows(dx)
                
            #保存            
            wb.save(workbook_file_path)
            
            hisIds = []
            #添加历史列表
            for _, t in enumerate(tasks):
                his_taskId = t[0]
                hisIds.append(his_taskId)
                self.addItemToExcel(t, file_name, history_sheet_name)     
                
            print(f"将任务Sheet({sheet_name})中的 过期任务 迁移指 -> 历史Sheet({history_sheet_name}) 完毕~ \n 迁移的任务ID为：{hisIds}")            
            
            #返回最新数据
            return self.readExcel()  
        else:
            print("timeTask文件不存在, 数据为空")
            self.create_excel()
            return []

    # 写入列表，返回元组列表
    def addItemToExcel(self, item, file_name=__file_name, sheet_name=__sheet_name):
        # 标准化时间字符串
        timeStr = item[2]
        task = TimeTaskModel(item, None, True)
        task.debug = True  # 手动开启调试
        item = list(item)
        item[2] = task.timeStr  # 确保timeStr已标准化
        item = tuple(item)
        
        # 文件路径
        workbook_file_path = self.get_file_path(file_name)
        
        # 如果文件存在,就执行
        if os.path.exists(workbook_file_path):
            wb = load_workbook(workbook_file_path)
            ws = wb[sheet_name]
            ws.append(item)
            wb.save(workbook_file_path)
            
            # 列表
            data = list(ws.values)
            #print(data)
            return data
        else:
            print("timeTask文件不存在, 添加数据失败")
            self.create_excel()
            return []

        
        
    # 写入数据
    def write_columnValue_withTaskId_toExcel(self, taskId, column: int, columnValue: str,  file_name=__file_name, sheet_name=__sheet_name):
        #读取数据
        data = self.readExcel(file_name, sheet_name)
        if len(data) > 0:
            # 表格对象
            workbook_file_path = self.get_file_path(file_name)
            wb = load_workbook(workbook_file_path)
            ws = wb[sheet_name]
            isExist = False
            taskContent = None
            #遍历
            for index, hisItem in enumerate(data):
                model = TimeTaskModel(hisItem, None, False)
                #ID是否相同
                if model.taskId == taskId:
                    #置为已消费：即0
                    ws.cell(index + 1, column).value = columnValue
                    isExist = True
                    taskContent = model
                    
            if isExist: 
                #保存
                wb.save(workbook_file_path)
            
            return isExist, taskContent
        else:
            print("timeTask文件无数据, 消费数据失败")
            return False, None
    
    
    #获取文件路径      
    def get_file_path(self, file_name=__file_name):
        # 文件路径
        current_file = os.path.abspath(__file__)
        current_dir = os.path.dirname(current_file)
        workbook_file_path = current_dir + "/" + self.__dir_name + "/" + file_name
        
        # 工作簿当前目录
        workbook_dir_path = os.path.dirname(workbook_file_path)
        # 创建目录
        if not os.path.exists(workbook_dir_path):
            # 创建工作簿路径,makedirs可以创建级联路径
            os.makedirs(workbook_dir_path)
            
        return workbook_file_path
        
    #更新用户ID  
    def update_userId(self, file_name=__file_name, sheet_name=__sheet_name):
        #是否重新登录了
        datas = self.readExcel(file_name, sheet_name)
        
        if len(datas) <= 0:
            return
            
        #模型数组
        tempArray : List[TimeTaskModel] = []
        #原始数据
        for item in datas:
            model = TimeTaskModel(item, None, False)
            tempArray.append(model)
            
        #id字典数组：将相同目标人的ID聚合为一个数组
        idsDic = {}
        groupIdsDic = {}
        for model in tempArray:
            #目标用户名称
            target_name = model.other_user_nickname
            #群聊
            if model.isGroup:
                if not target_name in groupIdsDic.keys():
                    groupIdsDic[target_name] = [model]
                else:
                    arr1 = groupIdsDic[target_name]
                    arr1.append(model)
                    groupIdsDic[target_name] = list(arr1) 
            else:
                #好友
                if not target_name in idsDic.keys():
                    idsDic[target_name] = [model]
                else:
                    arr2 = idsDic[target_name]
                    arr2.append(model)
                    idsDic[target_name] = list(arr2)
        
        #待更新的ID数组
        if len(idsDic) <= 0:
            return
        
        #原始ID ：新ID
        oldAndNewIDDic = self.getNewId(idsDic, groupIdsDic)
        if len(oldAndNewIDDic) <= 0:
            return
            
        #更新列表数据
        workbook_file_path = self.get_file_path(file_name)
        wb = load_workbook(workbook_file_path)
        ws = wb[sheet_name]
        excel_data = list(ws.values)
        #机器人ID
        robot_user_id = itchat.instance.storageClass.userName
        #遍历任务列表 - 更新数据
        for index, item in enumerate(excel_data):
            model = TimeTaskModel(item, None, False)
            #目标用户ID
            oldId = model.other_user_id
            newId = oldAndNewIDDic.get(oldId)
            #找到了
            if newId is not None and len(newId) > 0:
                model.other_user_id = newId
                #更新ID
                #from
                ws.cell(index + 1, 7).value = newId
                #to
                ws.cell(index + 1, 9).value = robot_user_id
                #other
                ws.cell(index + 1, 11).value = newId
                #替换原始信息中的ID
                #旧的机器人ID
                old_robot_userId = model.toUser_id
                #原始消息体
                originStr = model.originMsg
                #替换旧的目标ID
                newString = originStr.replace(oldId, newId)
                #替换机器人ID
                newString = newString.replace(old_robot_userId, robot_user_id)
                ws.cell(index + 1, 13).value = newString
                #等待写入
                time.sleep(0.05)
                      
        #保存            
        wb.save(workbook_file_path)
        
            
            
    #获取新的用户ID  
    def getNewId(self, idsDic, groupIdsDic):
        oldAndNewIDDic = {}
        #好友  
        friends = []
        #群聊
        chatrooms = []
        
        #好友处理
        if len(idsDic) > 0:   
            #好友处理
            try:
                #获取好友列表
                friends = itchat.get_friends(update=True)[0:]
            except ZeroDivisionError:
                # 捕获并处理 ZeroDivisionError 异常
                print("好友列表, 错误发生")
            
            #获取好友 -（id组装 旧 ： 新）
            for friend in friends:
                #id
                userName = friend["UserName"]
                NickName = friend["NickName"]
                modelArray = idsDic.get(NickName)
                #找到了好友
                if modelArray is not None and len(modelArray) > 0:
                    model : TimeTaskModel = modelArray[0]
                    oldId = model.other_user_id
                    if oldId != userName:
                        oldAndNewIDDic[oldId] = userName    
         
        #群聊处理  
        if len(groupIdsDic) > 0:          
            #群聊处理       
            try:
                #群聊 （id组装 旧 ：新）   
                chatrooms = itchat.get_chatrooms()
            except ZeroDivisionError:
                # 捕获并处理 ZeroDivisionError 异常
                print("群聊列表, 错误发生")
            
            #获取群聊 - 旧 ： 新
            for chatroom in chatrooms:
                #id
                userName = chatroom["UserName"]
                NickName = chatroom["NickName"]
                modelArray = groupIdsDic.get(NickName)
                #找到了群聊
                if modelArray is not None and len(modelArray) > 0:
                    model : TimeTaskModel = modelArray[0]
                    oldId = model.other_user_id
                    if oldId != userName:
                        oldAndNewIDDic[oldId] = userName
                       
        return oldAndNewIDDic         
        

#task模型        
class TimeTaskModel:
    #Item数据排序
    #0：ID - 唯一ID (自动生成，无需填写)
    #1：是否可用 - 0/1，0=不可用，1=可用
    #2：时间信息 - 格式为：HH:mm:ss
    #3：轮询信息 - 格式为：每天、每周N、YYYY-MM-DD
    #4：消息内容 - 消息内容
    #5：fromUser - 来源user
    #6：fromUserID - 来源user ID
    #7：toUser - 发送给的user
    #8：toUser id - 来源user ID
    #9：other_user_nickname - Other名称
    #10：other_user_id - otehrID
    #11：isGroup - 0/1，是否群聊； 0=否，1=是
    #12：原始内容 - 原始的消息体
    #13：今天是否被消息 - 每天会在凌晨自动重置
    def __init__(self, item, msg: ChatMessage, isNeedFormat: bool, isNeedCalculateCron=False):
        self.debug = False
        self.isNeedCalculateCron = isNeedCalculateCron
        self.taskId = item[0]
        self.enable = item[1] == "1"
        
        # 是否今日已被消费
        self.is_today_consumed = False
        
        #时间信息
        timeValue = item[2]
        tempTimeStr = ""
        if isinstance(timeValue, datetime):
            # 变量是 datetime.time 类型（Excel修改后，openpyxl会自动转换为该类型，本次做修正）
            tempTimeStr = timeValue.strftime("%H:%M:%S")
        elif isinstance(timeValue, str):
            tempTimeStr = timeValue
        else:
            # 其他类型
            print("其他类型时间，暂不支持")
        self.timeStr = tempTimeStr
        
        #日期
        dayValue = item[3]
        tempDayStr = ""
        if isinstance(dayValue, datetime):
            # 变量是 datetime.datetime 类型（Excel修改后，openpyxl会自动转换为该类型，本次做修正）
            tempDayStr = dayValue.strftime("%Y-%m-%d")
        elif isinstance(dayValue, str):
            tempDayStr = dayValue
        else:
            # 其他类型
            print("其他类型时间，暂不支持")
        self.circleTimeStr = tempDayStr
        
        #事件
        self.eventStr = item[4]
        
        #通过对象加载
        if msg is not None:
            self.fromUser = msg.from_user_nickname
            self.fromUser_id = msg.from_user_id
            
            # 修复群组信息记录 - 合并群信息
            if hasattr(msg, 'is_group') and msg.is_group:
                # 对于群消息,将所有群信息合并成一条
                group_info = []
                if msg.to_user_nickname:
                    group_info.append(msg.to_user_nickname)
                if msg.other_user_nickname:
                    group_info.append(msg.other_user_nickname)
                self.toUser = " | ".join(group_info)  # 使用|分隔不同群名
                self.toUser_id = msg.to_user_id
                self.other_user_nickname = ""  # 清空,因为已经合并到toUser中
                self.other_user_id = msg.other_user_id
                self.isGroup = True
            else:
                # 私聊消息保持不变
                self.toUser = msg.to_user_nickname
                self.toUser_id = msg.to_user_id
                self.other_user_nickname = msg.other_user_nickname
                self.other_user_id = msg.other_user_id
                self.isGroup = False
            self.originMsg = str(msg)
        else:
            #通过Item加载
            self.fromUser = item[5]
            self.fromUser_id = item[6]
            self.toUser = item[7]
            self.toUser_id = item[8]
            self.other_user_nickname = item[9]
            self.other_user_id = item[10]
            self.isGroup = item[11] == "1"
            self.originMsg = item[12]
            if len(item) > 13:
                self.is_today_consumed = item[13] == "1" 
        
        #容错
        emptStr = ""
        self.fromUser = emptStr if self.fromUser is None else self.fromUser
        self.fromUser_id = emptStr if self.fromUser_id is None else self.fromUser_id
        self.toUser = emptStr if self.toUser is None else self.toUser
        self.toUser_id = emptStr if self.toUser_id is None else self.toUser_id
        self.other_user_nickname = emptStr if self.other_user_nickname is None else self.other_user_nickname
        self.other_user_id = emptStr if self.other_user_id is None else self.other_user_id
        self.isGroup = False if self.isGroup is None else self.isGroup
        self.originMsg = emptStr if self.originMsg is None else self.originMsg   
        
        #cron表达式
        self.cron_expression = self.get_cron_expression()

        # 判断任务是否周期性
        self.is_periodic = self.is_periodic_task()  # 使用独立的方法判断周期性
        print(f'任务是否周期性: {self.is_periodic}')       

        #需要处理格式
        if isNeedFormat:
            #计算内容ID (使用不可变的内容计算，去除元素：enable 会变、originMsg中有时间戳)
            new_tuple = (self.timeStr, self.circleTimeStr, self.eventStr, self.fromUser, 
                         self.toUser, self.other_user_id, "1" if self.isGroup else "0")
            temp_content='_'.join(new_tuple)
            short_id = self.get_short_id(temp_content)
            if self.debug:
                logging.debug(f'任务内容：{temp_content}，唯一ID：{short_id}')
            self.taskId = short_id
            
            #周期、time
            #cron表达式
            if self.isCron_time():
                if self.debug:
                    logging.debug("使用cron表达式")
                
            else:
                #正常的周期、时间
                g_circle = self.get_cicleDay(self.circleTimeStr)
                g_time = self.get_time(self.timeStr)
                self.timeStr = g_time
                self.circleTimeStr = g_circle
                
        # 今日消费态优化
        if self.is_today_consumed:
            now = datetime.now()
            if now.hour == 0 and now.minute == 0:
                self.is_today_consumed = False
                if self.debug:
                    logging.debug(f"任务 {self.taskId} 的 is_today_consumed 已在凌晨重置")
            elif self.is_today() and (self.is_nowTime()[0] or self.is_featureTime()):
                self.is_today_consumed = False
                if self.debug:
                    logging.debug(f"任务 {self.taskId} 的 is_today_consumed 已因时间条件重置")
                
        #数组为空
        self.cron_today_times = []
        
        #计算cron今天的时间点
        if self.isNeedCalculateCron and self.isCron_time() and self.enable:
            # 创建子线程
            t = threading.Thread(target=self.get_todayCron_times)
            t.setDaemon(True) 
            t.start() 
     
    #获取今天cron时间  
    def get_todayCron_times(self):
        if not self.enable:
              return
          
        self.cron_today_times = []
        #校验cron格式
        if self.isValid_Cron_time():
            # 获取当前时间（忽略秒数）
            current_time = arrow.now().replace(second=0, microsecond=0)
            # 创建一个 croniter 对象
            cron = croniter(self.cron_expression, current_time.datetime)
            next_time = cron.get_next(datetime)
            while next_time.date() == current_time.date():
                #记录时间（时：分）
                next_time_hour_minut = next_time.strftime('%H:%M')
                self.cron_today_times.append(next_time_hour_minut)
                next_time = cron.get_next(datetime)
            
            #打印满足今天的cron的时间点    
            print(f"cron表达式为：{self.cron_expression}, 满足今天的时间节点为：{self.cron_today_times}")
        
    #获取格式化后的Item
    def get_formatItem(self):
        temp_item = (self.taskId,
                "1" if self.enable else "0",
                self.timeStr,
                self.circleTimeStr,
                self.eventStr,
                self.fromUser,
                self.fromUser_id,
                self.toUser,
                self.toUser_id,
                self.other_user_nickname,
                self.other_user_id,
                "1" if self.isGroup else "0",
                self.originMsg,
                "1" if self.is_today_consumed else "0") 
        return temp_item
            
    #计算唯一ID        
    def get_short_id(self, string):
        # 使用 MD5 哈希算法计算字符串的哈希值
        hash_value = hashlib.md5(string.encode()).digest()
    
        # 将哈希值转换为一个 64 进制的短字符串
        short_id = base64.urlsafe_b64encode(hash_value)[:8].decode()
        return short_id
    
    
    #判断是否当前时间    
    def is_nowTime(self):
        """判断是否当前时间，返回(是否当前时间, 当前时间字符串)"""
        tempTimeStr = self.timeStr
        if not tempTimeStr:
            return False, ""
            
        if tempTimeStr.count(":") == 1:
           tempTimeStr = tempTimeStr + ":00"
        
        #cron   
        if self.isCron_time():
            current_time = arrow.now().replace(second=0, microsecond=0)
            return True, current_time.format('HH:mm')
        else:    
            #对比精准到分（忽略秒）
            current_time = arrow.now().replace(second=0, microsecond=0)
            task_time = arrow.get(tempTimeStr, "HH:mm:ss").replace(second=0, microsecond=0)
            is_now = task_time.time() == current_time.time()
            return is_now, current_time.format('HH:mm')
    
    #判断是否未来时间    
    def is_featureTime(self):
        """判断是否未来时间"""
        tempTimeStr = self.timeStr
        if not tempTimeStr:
            return False
            
        if tempTimeStr.count(":") == 1:
           tempTimeStr = tempTimeStr + ":00"
        
        #cron   
        if self.isCron_time():
            return True 
        else:    
            #对比精准到分（忽略秒）
            current_time = arrow.now().replace(second=0, microsecond=0).time()
            task_time = arrow.get(tempTimeStr, "HH:mm:ss").replace(second=0, microsecond=0).time()
            tempValue = task_time > current_time
            return tempValue 
    
    #判断是否未来日期    
    def is_featureDay(self):
        """判断是否未来日期"""
        # cron 表达式任务总是返回 True
        if self.isCron_time():
            return True

        tempStr = self.circleTimeStr

        # 对于"每天"和"工作日"这样的周期性任务,永远返回 True
        if tempStr in ["每天", "工作日"]:
            return True

        # 对于每周X的任务,也返回 True
        if tempStr.startswith('每周') or tempStr.startswith('每星期'):
            return True

        # 对于具体日期,判断是否是未来日期
        if self.is_valid_date(tempStr):
            return arrow.get(tempStr, 'YYYY-MM-DD').date() > arrow.now().date()

        return False

        
        tempStr = self.circleTimeStr
        
        # 对于"每天"和"工作日"这样的周期性任务,永远返回True
        if tempStr in ["每天", "工作日"]:
            return True
            
        # 对于每周X的任务,也返回True
        if tempStr.startswith('每周') or tempStr.startswith('每星期'):
            return True
            
        # 对于具体日期,判断是否是未来日期
        if self.is_valid_date(tempStr):
            return arrow.get(tempStr, 'YYYY-MM-DD').date() > arrow.now().date()
            
        return False
    
    #判断是否今天    
    def is_today(self):
        """判断任务是否今天"""
        logging.debug("is_today: 判断任务是否今天")
        try:
            tempStr = self.circleTimeStr
            logging.debug(f"is_today: circleTimeStr='{tempStr}'")
        
            # 对于"每天",永远返回 True
            if tempStr == "每天":
                logging.debug("is_today: 任务是每天执行。返回 True。")
                return True
        
            # 对于"工作日",判断今天是否是工作日(周一至周五)
            if tempStr == "工作日":
                today = arrow.now()
                is_weekday = 0 <= today.weekday() <= 4
                logging.debug(f"is_today: 任务是工作日，今天是否工作日: {is_weekday}")
                return is_weekday
        
            # 处理每周X的格式
            if tempStr.startswith('每周') or tempStr.startswith('每星期'):
                weekday = tempStr[-1]  # 获取最后一个字符
                is_today = self.is_today_weekday(weekday)
                logging.debug(f"is_today: 任务是每周任务，今天是否为指定星期几: {is_today}")
                return is_today
        
            # 处理具体日期
            if self.is_valid_date(tempStr):
                today = arrow.now().format('YYYY-MM-DD')
                is_today = tempStr == today
                logging.debug(f"is_today: 任务是具体日期，是否今天: {is_today}")
                return is_today

            logging.debug("is_today: 任务既不是周期性任务，也不是具体日期。返回 False。")
            return False

        except Exception as e:
            if self.debug:
                logging.debug(f"is_today: 检查任务日期时发生错误: {str(e)}")
            return False

    def is_periodic_task(self) -> bool:
        """
        判断任务是否为周期性任务。
        """
        periodic_keywords = ["每天", "工作日"]
        if self.circleTimeStr in periodic_keywords:
            logging.debug(f"is_periodic_task: '{self.circleTimeStr}' 是周期性关键字。返回 True。")
            return True
        if self.circleTimeStr.startswith('每周') or self.circleTimeStr.startswith('每星期'):
            logging.debug(f"is_periodic_task: '{self.circleTimeStr}' 是每周任务。返回 True。")
            return True
        if self.isCron_time():
            logging.debug(f"is_periodic_task: 任务是 cron 表达式。返回 True。")
            return True
        logging.debug(f"is_periodic_task: '{self.circleTimeStr}' 不是周期性任务。返回 False。")
        return False

    def process_task(task: TimeTaskModel):
        try:
            if task.is_periodic:
                print(f"处理周期性任务 {task.taskId}")
                # 调度周期性任务，例如每天
                schedule_periodic_task(task)
            else:
                print(f"处理一次性任务 {task.taskId}")
                # 处理一次性任务，解析具体日期
                task_date = datetime.strptime(task.circleTimeStr, '%Y-%m-%d')
                schedule_one_time_task(task, task_date)
        except Exception as e:
            print(f"处理任务 {task.taskId} 时出错: {e}")
    
    #判断是否今天的星期数    
    def is_today_weekday(self, weekday_str):
        """判断是否今天的星期数"""
        # 将中文数字转换为阿拉伯数字
        weekday_dict = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '日': 7, '天': 7}
        weekday_num = weekday_dict.get(weekday_str[-1])
        if weekday_num is None:
            return False
        
        # 判断今天是否是指定的星期几
        today = arrow.now()
        tempValue = today.weekday() == weekday_num - 1   
        return tempValue   
        
    #判断日期格式是否正确    
    def is_valid_date(self, date_string):
        """检查日期格式是否正确"""
        if not date_string:
            logging.debug("is_valid_date: date_string为空。")
            return False

        logging.debug(f"is_valid_date: 检查 date_string='{date_string}'")
        
        # 不再将'每天'等视为有效日期
        if date_string in ['今天', '明天', '后天', '每天', '工作日']:
            logging.debug(f"is_valid_date: '{date_string}' 是周期性关键字。返回 False。")
            return False

        # 不再将每周X的格式视为有效日期
        if date_string.startswith('每周') or date_string.startswith('每星期'):
            logging.debug(f"is_valid_date: '{date_string}' 以 '每周' 或 '每星期' 开头。返回 False。")
            return False

        # 不再将cron表达式视为有效日期
        if date_string.startswith("cron["):
            logging.debug(f"is_valid_date: '{date_string}' 是cron表达式。返回 False。")
            return False

        try:
            # 尝试解析完整时间戳格式 YYYY-MM-DD HH:mm:ss
            datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
            logging.debug(f"is_valid_date: '{date_string}' 匹配 '%Y-%m-%d %H:%M:%S'。返回 True。")
            return True
        except ValueError:
            try:
                # 尝试解析标准日期格式 YYYY-MM-DD
                datetime.strptime(date_string, '%Y-%m-%d')
                logging.debug(f"is_valid_date: '{date_string}' 匹配 '%Y-%m-%d'。返回 True。")
                return True
            except ValueError:
                logging.debug(f"is_valid_date: '{date_string}' 不匹配任何日期格式。返回 False。")
                return False



    #获取周期
    def get_cicleDay(self, circleStr):
        """获取格式化的日期"""
        if not circleStr:
            logging.debug("get_cicleDay: circleStr为空。")
            return ""

        logging.debug(f"get_cicleDay: 处理 circleStr='{circleStr}'")
        
        # 直接返回特殊周期字符串
        if circleStr in ["每天", "工作日"]:
            logging.debug(f"get_cicleDay: '{circleStr}' 是特殊周期关键字。直接返回。")
            return circleStr
        
        # 处理今天、明天、后天
        if circleStr == "今天":
            formatted_date = arrow.now().format('YYYY-MM-DD')
            logging.debug(f"get_cicleDay: '今天' 转换为 '{formatted_date}'")
            return formatted_date
        elif circleStr == "明天":
            formatted_date = arrow.now().shift(days=1).format('YYYY-MM-DD')
            logging.debug(f"get_cicleDay: '明天' 转换为 '{formatted_date}'")
            return formatted_date
        elif circleStr == "后天":
            formatted_date = arrow.now().shift(days=2).format('YYYY-MM-DD')
            logging.debug(f"get_cicleDay: '后天' 转换为 '{formatted_date}'")
            return formatted_date
        
        # 处理每周X的格式    
        if circleStr.startswith('每周') or circleStr.startswith('每星期'):
            logging.debug(f"get_cicleDay: '{circleStr}' 是每周任务。直接返回。")
            return circleStr
        
        # 处理标准日期格式
        try:
            # 尝试解析完整时间戳格式 YYYY-MM-DD HH:mm:ss
            temp_date = datetime.strptime(circleStr, '%Y-%m-%d %H:%M:%S')
            formatted_date = temp_date.strftime('%Y-%m-%d')
            logging.debug(f"get_cicleDay: '{circleStr}' 解析为 '{formatted_date}'")
            return formatted_date
        except ValueError:
            try:
                # 尝试解析标准日期格式 YYYY-MM-DD
                temp_date = datetime.strptime(circleStr, '%Y-%m-%d')
                formatted_date = temp_date.strftime('%Y-%m-%d')
                logging.debug(f"get_cicleDay: '{circleStr}' 解析为 '{formatted_date}'")
                return formatted_date
            except ValueError:
                logging.debug(f"get_cicleDay: '{circleStr}' 解析失败。")
                return ""

    
    def get_time(self, timeStr):
        """获取格式化的时间"""
        if not timeStr:
            logging.debug("get_time: timeStr为空。")
            return ""
            
        logging.debug(f"get_time: 正在处理时间字符串: '{timeStr}'")
        
        g_time = ""
        # 允许一位或两位小时的正则表达式
        pattern1 = r'^\d{1,2}:\d{2}:\d{2}$'
        pattern2 = r'^\d{1,2}:\d{2}$'

        try:
            # 如果是cron表达式，直接返回
            if timeStr.startswith("cron["):
                logging.debug(f"get_time: '{timeStr}' 是cron表达式。直接返回。")
                return timeStr
                
            # 尝试解析完整时间戳格式
            if " " in timeStr:
                try:
                    dt = datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
                    g_time = dt.strftime("%H:%M:%S")
                    logging.debug(f"get_time: '{timeStr}' 解析为 '{g_time}'")
                    return g_time
                except ValueError:
                    logging.debug(f"get_time: '{timeStr}' 不是 '%Y-%m-%d %H:%M:%S' 格式。继续尝试其他格式。")
                    pass
                
            # 尝试解析标准时间格式
            if re.match(pattern1, timeStr):
                # 如果格式完整，标准化为两位小时
                dt = datetime.strptime(timeStr, "%H:%M:%S")
                g_time = dt.strftime("%H:%M:%S")
                logging.debug(f"get_time: '{timeStr}' 匹配 '{pattern1}'，标准化为 '{g_time}'")
            elif re.match(pattern2, timeStr):
                # 如果只有时分，添加秒并标准化
                dt = datetime.strptime(timeStr, "%H:%M")
                g_time = dt.strftime("%H:%M:%S")
                logging.debug(f"get_time: '{timeStr}' 匹配 '{pattern2}'，添加秒后为 '{g_time}'")
            else:
                # 处理中文时间描述
                try:
                    # 预处理时间字符串
                    content = timeStr.replace("早上", "").replace("上午", "").replace("中午", "").replace("下午", "").replace("晚上", "")
                    content = content.replace("点", ":").replace("分", ":").replace("秒", "")
                    
                    # 中文数字映射
                    digits = {
                        '零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
                        '十一': 11, '十二': 12, '十三': 13, '十四': 14, '十五': 15, '十六': 16, '十七': 17, '十八': 18, '十九': 19, '二十': 20,
                        '二十一': 21, '二十二': 22, '二十三': 23, '二十四': 24
                    }
                    
                    # 分解时间部分
                    parts = content.split(":")
                    hour = "0"
                    minute = "0"
                    second = "0"
                    
                    # 处理小时
                    if len(parts) > 0 and parts[0]:
                        if parts[0] in digits:
                            hour = str(digits[parts[0]])
                            logging.debug(f"get_time: 解析小时部分 '{parts[0]}' 为 {hour}")
                        else:
                            try:
                                hour = str(int(parts[0]))
                                logging.debug(f"get_time: 解析小时部分 '{parts[0]}' 为 {hour}")
                            except ValueError:
                                logging.debug(f"get_time: 无法解析小时部分 '{parts[0]}'")
                                return ""
                                
                    # 处理分钟
                    if len(parts) > 1 and parts[1]:
                        if parts[1] in digits:
                            minute = str(digits[parts[1]])
                            logging.debug(f"get_time: 解析分钟部分 '{parts[1]}' 为 {minute}")
                        else:
                            try:
                                minute = str(int(parts[1]))
                                logging.debug(f"get_time: 解析分钟部分 '{parts[1]}' 为 {minute}")
                            except ValueError:
                                minute = "0"
                                logging.debug(f"get_time: 无法解析分钟部分 '{parts[1]}'，默认为 '0'")
                                
                    # 处理秒
                    if len(parts) > 2 and parts[2]:
                        if parts[2] in digits:
                            second = str(digits[parts[2]])
                            logging.debug(f"get_time: 解析秒部分 '{parts[2]}' 为 {second}")
                        else:
                            try:
                                second = str(int(parts[2]))
                                logging.debug(f"get_time: 解析秒部分 '{parts[2]}' 为 {second}")
                            except ValueError:
                                second = "0"
                                logging.debug(f"get_time: 无法解析秒部分 '{parts[2]}'，默认为 '0'")
                                
                    # 处理时间段
                    if "中午" in timeStr:
                        if int(hour) < 12:
                            hour = "12"
                            logging.debug(f"get_time: 处理中午时间段，将小时 '{hour}' 设置为 '12'")
                    elif "下午" in timeStr or "晚上" in timeStr:
                        if int(hour) < 12:
                            hour = str(int(hour) + 12)
                            logging.debug(f"get_time: 处理下午/晚上时间段，将小时 '{hour}' 增加为 '{hour}'")
                            
                    # 格式化时间
                    hour = f"0{hour}" if int(hour) < 10 else hour
                    minute = f"0{minute}" if int(minute) < 10 else minute
                    second = f"0{second}" if int(second) < 10 else second
                    
                    g_time = f"{hour}:{minute}:{second}"
                    
                    if self.debug:
                        logging.debug(f"get_time: 转换中文时间: '{timeStr}' -> '{g_time}'")
                    
                except Exception as e:
                    if self.debug:
                        logging.debug(f"get_time: 解析中文时间失败: {str(e)}")
                    return ""
            
            # 验证最终时间格式
            if re.match(pattern1, g_time):
                if self.debug:
                    logging.debug(f"get_time: 最终格式化时间符合预期: '{g_time}'")
                return g_time
            else:
                if self.debug:
                    logging.debug(f"get_time: 格式化时间不符合预期: '{g_time}'")
                return ""
            
        except Exception as e:
            if self.debug:
                logging.debug(f"get_time: 时间转换错误: {str(e)}")
            return ""


            
            # 验证最终时间格式
            if re.match(pattern1, g_time):
                return g_time
            return ""
            
        except Exception as e:
            if self.debug:
                logging.debug(f"时间转换错误: {str(e)}")
            return ""
    
    #是否 cron表达式
    def isCron_time(self):
        tempValue = self.circleTimeStr.startswith("cron[")
        return tempValue
    
    #是否正确的cron格式
    def isValid_Cron_time(self):
        tempValue = croniter.is_valid(self.cron_expression)
        return tempValue
    
    #获取 cron表达式
    def get_cron_expression(self):
        if self.circleTimeStr == "每天":
            # 每天在指定时间执行
            seconds = int(self.timeStr.split(':')[2])
            minutes = int(self.timeStr.split(':')[1])
            hours = int(self.timeStr.split(':')[0])
            cron_expr = f"{seconds} {minutes} {hours} * * *"
            print(f"生成每天的 cron 表达式: '{cron_expr}'")
            return cron_expr
        elif re.match(r'^每周[一二三四五六日天]$', self.circleTimeStr) or re.match(r'^每星期[一二三四五六日天]$', self.circleTimeStr):
            # 解析星期几
            weekday_map = {'一':1, '二':2, '三':3, '四':4, '五':5, '六':6, '日':7, '天':7}
            weekday_char = self.circleTimeStr[-1]
            weekday_num = weekday_map.get(weekday_char, '*')
            seconds = int(self.timeStr.split(':')[2])
            minutes = int(self.timeStr.split(':')[1])
            hours = int(self.timeStr.split(':')[0])
            cron_expr = f"{seconds} {minutes} {hours} * * {weekday_num}"
            print(f"生成每周的 cron 表达式: '{cron_expr}'")
            return cron_expr
        else:
            # 处理已有的cron表达式或其他格式
            cron_expr = self.timeStr.replace("cron[", "").replace("Cron[", "").replace("]", "")
            print(f"使用已有的 cron 表达式: '{cron_expr}'")
            return cron_expr


    
    #是否 私聊制定群任务
    def isPerson_makeGrop(self):
        tempValue = self.eventStr.endswith("]")
        tempValue1 = "group[" in self.eventStr or "Group[" in self.eventStr
        return tempValue and tempValue1
    
    #获取私聊制定群任务的群Title、事件
    def get_Persion_makeGropTitle_eventStr(self):
        index = -1
        targetStr = self.eventStr
        if "group[" in targetStr:
            index = targetStr.index("group[")
        elif "Group[" in targetStr:
            index = targetStr.index("Group[")
        if index < 0:
              return "", targetStr
          
        substring_event = targetStr[:index].strip()
        substring_groupTitle = targetStr[index + 6:]
        substring_groupTitle = substring_groupTitle.replace("]", "").strip()
        return substring_event, substring_groupTitle
    
    #通过 群Title 获取群ID
    def get_gropID_withGroupTitle(self, groupTitle, channel_name):
        """通过群标题获取群ID"""
        if len(groupTitle) <= 0:
              return ""
              
        print(f"[{channel_name}通道] 开始查找群【{groupTitle}】")
        # 转换为小写以进行大小写不敏感匹配
        groupTitle_lower = groupTitle.lower()
        
        #itchat
        if channel_name == "wx":
            tempRoomId = ""
            #群聊处理       
            try:
                #群聊  
                chatrooms = itchat.get_chatrooms(update=True)  # 添加update=True强制更新群列表
                print(f"[{channel_name}通道] 当前共有 {len(chatrooms)} 个群")
                
                #获取群聊
                for chatroom in chatrooms:
                    #id
                    userName = chatroom["UserName"]
                    NickName = chatroom["NickName"]
                    print(f"[{channel_name}通道] 正在检查群：{NickName}")
                    # 转换为小写进行精确匹配
                    nickName_lower = NickName.lower()
                    # 使用精确匹配（只忽略大小写）
                    if groupTitle_lower == nickName_lower:
                        tempRoomId = userName
                        print(f"[{channel_name}通道] 找到匹配的群：{NickName}，ID：{userName}")
                        break
                    
                if not tempRoomId:
                    print(f"[{channel_name}通道] 未找到群【{groupTitle}】，当前所有群：")
                    for room in chatrooms:
                        print(f"  - {room['NickName']}")
                        
            except Exception as e:
                print(f"[{channel_name}通道] 通过群标题获取群ID时发生错误：{str(e)}")
                print(f"[{channel_name}通道] 错误详情：", e)
                return ""
                
            return tempRoomId

        elif channel_name == "ntchat":
            tempRoomId = ""
            try:
                #数据结构为字典数组
                rooms = wechatnt.get_rooms()
                print(f"[{channel_name}通道] 当前共有 {len(rooms)} 个群")
                
                if len(rooms) > 0:
                    #遍历
                    for item in rooms:
                        roomId = item.get("wxid")
                        nickname = item.get("nickname")
                        print(f"[{channel_name}通道] 正在检查群：{nickname}")
                        # 转换为小写进行精确匹配
                        nickname_lower = nickname.lower()
                        # 使用精确匹配（只忽略大小写）
                        if groupTitle_lower == nickname_lower:
                            tempRoomId = roomId
                            print(f"[{channel_name}通道] 找到匹配的群：{nickname}，ID：{roomId}")
                            break
                            
                if not tempRoomId:
                    print(f"[{channel_name}通道] 未找到群【{groupTitle}】，当前所有群：")
                    for room in rooms:
                        print(f"  - {room.get('nickname')}")
                return tempRoomId
                        
            except Exception as e:
                print(f"[{channel_name}通道] 通过群标题获取群ID时发生错误：{str(e)}")
                print(f"[{channel_name}通道] 错误详情：", e)
                return tempRoomId

        elif channel_name == "wework":
            tempRoomId = ""
            try:
                # 数据结构为字典数组
                rooms = wework.get_rooms().get("room_list")
                print(f"[{channel_name}通道] 当前共有 {len(rooms)} 个群")
                
                if len(rooms) > 0:
                    # 遍历
                    for item in rooms:
                        roomId = item.get("conversation_id")
                        nickname = item.get("nickname")
                        print(f"[{channel_name}通道] 正在检查群：{nickname}")
                        # 转换为小写进行精确匹配
                        nickname_lower = nickname.lower()
                        # 使用精确匹配（只忽略大小写）
                        if groupTitle_lower == nickname_lower:
                            tempRoomId = roomId
                            print(f"[{channel_name}通道] 找到匹配的群：{nickname}，ID：{roomId}")
                            break
                            
                if not tempRoomId:
                    print(f"[{channel_name}通道] 未找到群【{groupTitle}】，当前所有群：")
                    for room in rooms:
                        print(f"  - {room.get('nickname')}")
                        
            except Exception as e:
                print(f"[{channel_name}通道] 通过群标题获取群ID时发生错误：{str(e)}")
                print(f"[{channel_name}通道] 错误详情：", e)
                return ""
                
            return tempRoomId

        else:
            print(f"[{channel_name}通道] 不支持通过群标题获取群ID，当前channel：{channel_name}")
            return ""

class CleanFiles:
    def __init__(self, save_path):
        self.save_path = save_path

    def clean_expired_files(self, days=3):
        """清理过期文件"""
        try:
            # 使用更灵活的时间格式解析
            current_time = datetime.now()
            expire_time = current_time - timedelta(days=days)
            
            # 遍历目录
            for root, dirs, files in os.walk(self.save_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        # 获取文件修改时间
                        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_time < expire_time:
                            try:
                                os.remove(file_path)
                                logger.info(f"已删除过期文件: {file_path}")
                            except Exception as e:
                                logger.error(f"删除文件失败 {file_path}: {str(e)}")
                    except Exception as e:
                        logger.error(f"获取文件时间失败 {file_path}: {str(e)}")
        except Exception as e:
            logger.error(f"清理过期文件出错: {str(e)}")
