# encoding:utf-8

from plugins.timetask.Tool import ExcelTool
from plugins.timetask.Tool import TimeTaskModel
import logging
import time
import arrow
import threading
import os
from typing import List
from plugins.timetask.config import conf, load_config
from lib import itchat
from lib.itchat.content import *
import config as RobotConfig
try:
    from channel.wechatnt.ntchat_channel import wechatnt
except Exception as e:
    print(f"未安装ntchat: {e}")

class TaskManager(object):
    
    def __init__(self, timeTaskFunc):
        super().__init__()
        #保存定时任务回调
        self.timeTaskFunc = timeTaskFunc
        
        # 初始化任务锁集合
        self._task_locks = set()
        
        # 创建子线程
        t = threading.Thread(target=self.pingTimeTask_in_sub_thread)
        t.setDaemon(True) 
        t.start()
        
    # 定义子线程函数
    def pingTimeTask_in_sub_thread(self):
        #延迟5秒后再检测，让初始化任务执行完
        time.sleep(5)
        
        #检测是否重新登录了
        self.isRelogin = False
        
        #迁移任务的标识符：用于标识在目标时间，只迁移一次
        self.moveHistoryTask_identifier = ""
        
        #刷新任务的标识符：用于标识在目标时间，只刷新一次
        self.refreshTimeTask_identifier = ""
        
        #存放历史数据
        self.historyTasks = []
        
        #配置加载
        load_config()
        self.conf = conf()
        self.debug = self.conf.get("debug", False)
        
        #迁移任务的时间
        self.move_historyTask_time = self.conf.get("move_historyTask_time", "04:00:00")
        #默认每秒检测一次
        self.time_check_rate = self.conf.get("time_check_rate", 1)
        
        #excel创建
        obj = ExcelTool()
        obj.create_excel()
        #任务数组
        self.refreshDataFromExcel()
        #过期任务数组、现在待消费数组、未来任务数组
        historyArray, _, _ = self.getFuncArray(self.timeTasks)
        #启动时，默认迁移一次过期任务
        self.moveTask_toHistory(historyArray)
        
        #循环
        while True:
            # 定时检测
            self.timeCheck()
            time.sleep(int(self.time_check_rate))
    
    #时间检查
    def timeCheck(self):
        
        #检测是否重新登录了
        self.check_isRelogin()
        #重新登录、未登录，均跳过
        if self.isRelogin:
            return
        
        #过期任务数组、现在待消费数组、未来任务数组
        modelArray = self.timeTasks
        historyArray, currentExpendArray, featureArray = self.getFuncArray(modelArray)
        
        #存放历史数据
        if len(historyArray) > 0:
            for item in historyArray:
                if item not in currentExpendArray and item not in featureArray and item not in self.historyTasks:
                      self.historyTasks.append(item)
        
        #是否到了凌晨00:00 - 目标时间，刷新今天的cron任务
        if self.is_targetTime("00:00"):
            #刷新cron时间任务、周期任务的今天执行态
            self.refresh_times(featureArray) 
        elif len(self.refreshTimeTask_identifier) > 0:
            self.refreshTimeTask_identifier = ""
            
        
        #是否到了迁移历史任务 - 目标时间
        if self.is_targetTime(self.move_historyTask_time):
            #迁移过期任务
            self.moveTask_toHistory(self.historyTasks)
        elif len(self.moveHistoryTask_identifier) > 0:
            self.moveHistoryTask_identifier = ""
            
        #任务数组
        if len(modelArray) <= 0:
            return
                    
        #将数组赋值数组，提升性能(若self.timeTasks 未被多线程更新，赋值为待执行任务组)
        timeTask_ids = '😄'.join(item.taskId for item in self.timeTasks)
        modelArray_ids = '😄'.join(item.taskId for item in modelArray)
        featureArray_ids = '😄'.join(item.taskId for item in featureArray)
        if timeTask_ids == modelArray_ids and timeTask_ids != featureArray_ids:
            #将任务数组 更新为 待执行数组； 当前任务在下面执行消费逻辑
            self.timeTasks = featureArray
            if self.debug:
                logging.debug(f"内存任务更新：原任务列表 -> 待执行任务列表")
                logging.debug(f"原任务ID列表：{timeTask_ids}")
                logging.debug(f"待执行任务ID列表：{featureArray_ids}")
        
        #当前无待消费任务     
        if len(currentExpendArray) <= 0:
            if self.debug:
                logging.debug("[timetask][定时检测]：当前时刻 - 无定时任务...")
            return
        
        # 使用更精确的时间戳作为锁标识
        current_timestamp = arrow.now().format('YYYY-MM-DD HH:mm:ss')

        # 创建文件锁目录（如果不存在）
        lock_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task_locks")
        if not os.path.exists(lock_dir):
            os.makedirs(lock_dir)

        # 使用文件锁检查任务是否已执行
        for task in currentExpendArray[:]:
            # 使用任务ID和分钟时间戳创建锁文件名
            lock_file = os.path.join(lock_dir, f"{task.taskId}_{arrow.now().format('YYYY-MM-DD_HH-mm')}.lock")

            try:
                # 尝试创建锁文件
                if os.path.exists(lock_file):
                    if self.debug:
                        logging.debug(f"任务 {task.taskId} 在当前时间 {current_timestamp} 已执行，跳过")
                    currentExpendArray.remove(task)
                    continue

                # 创建锁文件并写入时间戳
                with open(lock_file, 'w') as f:
                    f.write(current_timestamp)

            except Exception as e:
                logging.error(f"处理任务锁时出错: {str(e)}")
                continue

        # 清理过期的锁文件（保留最近30分钟的）
        try:
            current_time = arrow.now()
            for lock_file in os.listdir(lock_dir):
                try:
                    file_path = os.path.join(lock_dir, lock_file)
                    # 使用文件修改时间而不是文件名来判断过期
                    file_mtime = arrow.get(os.path.getmtime(file_path))
                    
                    if (current_time - file_mtime).total_seconds() > 1800:  # 30分钟 = 1800秒
                        os.remove(file_path)
                        if self.debug:
                            logging.debug(f"已删除过期锁文件: {file_path}")
                except Exception as e:
                    if self.debug:
                        logging.debug(f"处理锁文件时出错 {lock_file}: {str(e)}")
                    continue
        except Exception as e:
            logging.error(f"清理过期锁文件时出错: {str(e)}")

        # 消费当前task
        if len(currentExpendArray) > 0:
            logging.info(f"[timetask][定时检测]：当前时刻 - 存在定时任务, 执行消费 当前时刻任务")
            self.runTaskArray(currentExpendArray)


    #检测是否重新登录了    
    def check_isRelogin(self):
        #机器人ID
        robot_user_id = ""
        #通道
        channel_name = RobotConfig.conf().get("channel_type", "wx")
        if channel_name == "wx":
            robot_user_id = itchat.instance.storageClass.userName
        elif channel_name == "ntchat":
            try:
                login_info = wechatnt.get_login_info()
                nickname = login_info['nickname']
                user_id = login_info['wxid']
                robot_user_id = user_id
            except Exception as e:
                print(f"获取 ntchat的 userid 失败: {e}")
                #nt
                self.isRelogin = False
                return  
        else:
            #其他通道，默认不更新用户ID
            self.isRelogin = False
            return  
        
        #登录后
        if robot_user_id is not None and len(robot_user_id) > 0:
            #NTChat的userID不变  
            if channel_name == "ntchat":
                self.isRelogin = False
                return  
        
            #取出任务中的一个模型
            if self.timeTasks is not None and len(self.timeTasks) > 0: 
                model : TimeTaskModel = self.timeTasks[0]
                temp_isRelogin = robot_user_id != model.toUser_id
            
                if temp_isRelogin:
                    #更新为重新登录态
                    self.isRelogin = True
                    #等待登录完成
                    time.sleep(3)
                    
                    #更新userId
                    ExcelTool().update_userId()
                    #刷新数据
                    self.refreshDataFromExcel()
                    
                    #更新为非重新登录态
                    self.isRelogin = False
        else:
            #置为重新登录态
            self.isRelogin = True      
        
            
    #拉取Excel最新数据    
    def refreshDataFromExcel(self):
        tempArray = ExcelTool().readExcel()
        self.convetDataToModelArray(tempArray) 
        
    #迁移历史任务   
    def moveTask_toHistory(self, modelArray):
        if len(modelArray) <= 0:
            return
          
        #当前时间的小时：分钟
        current_time_hour_min = arrow.now().format('HH:mm')
        #执行中 - 标识符
        identifier_running = f"{current_time_hour_min}_running"
        #结束 - 标识符
        identifier_end = f"{current_time_hour_min}_end"
        
        #当前状态
        current_task_state = self.moveHistoryTask_identifier
        
        #未执行
        if current_task_state == "":
            #打印当前任务
            new_array = [item.taskId for item in self.timeTasks]
            print(f"[timeTask] 触发了迁移历史任务~ 当前任务ID为：{new_array}")
            
            #置为执行中
            self.moveHistoryTask_identifier = identifier_running
            #迁移任务
            newTimeTask = ExcelTool().moveTasksToHistoryExcel(modelArray)
            #数据刷新
            self.convetDataToModelArray(newTimeTask)
            
        #执行中    
        elif current_task_state == identifier_running:
            return
        
        #执行完成
        elif current_task_state == identifier_end:
            self.moveHistoryTask_identifier == ""
            
        #容错：如果时间未跳动，则正常命中【执行完成】； 异常时间跳动时，则比较时间
        elif "_end" in current_task_state:
            #标识符中的时间
            tempTimeStr = current_task_state.replace("_end", ":00")
            current_time = arrow.now().replace(second=0, microsecond=0).time()
            task_time = arrow.get(tempTimeStr, "HH:mm:ss").replace(second=0, microsecond=0).time()
            tempValue = task_time < current_time
            if tempValue:
                self.moveHistoryTask_identifier == ""
                
                
    #刷新c任务   
    def refresh_times(self, modelArray):
        #当前时间的小时：分钟
        current_time_hour_min = arrow.now().format('HH:mm')
        #执行中 - 标识符
        identifier_running = f"{current_time_hour_min}_running"
        #结束 - 标识符
        identifier_end = f"{current_time_hour_min}_end"
        
        #当前状态
        current_task_state = self.refreshTimeTask_identifier
        
        if self.debug:
            logging.debug(f"开始检查任务刷新状态")
            logging.debug(f"当前时间: {current_time_hour_min}")
            logging.debug(f"当前状态: {current_task_state}")
        
        try:
            #未执行
            if current_task_state == "":
                #打印此时任务
                new_array = [item.taskId for item in self.timeTasks]
                if self.debug:
                    logging.debug(f"触发凌晨刷新任务，当前任务列表：{new_array}")
                
                #置为执行中
                self.refreshTimeTask_identifier = identifier_running
                if self.debug:
                    logging.debug(f"状态更新为：执行中")
                
                #刷新任务
                for m in modelArray:
                    taskModel : TimeTaskModel = m
                    if self.debug:
                        logging.debug(f"重置任务 {taskModel.taskId} 的消费状态")
                    taskModel.is_today_consumed = False
                    ExcelTool().write_columnValue_withTaskId_toExcel(taskModel.taskId, 14, "0")
                
                #刷新数据
                self.refreshDataFromExcel()
                if self.debug:
                    logging.debug(f"已刷新内存中的任务数据")
                
                #设置完成标识
                self.refreshTimeTask_identifier = identifier_end
                if self.debug:
                    logging.debug(f"凌晨刷新任务完成")
                
            #执行中    
            elif current_task_state == identifier_running:
                if self.debug:
                    logging.debug(f"任务正在执行中，跳过")
                return
            
            #执行完成
            elif current_task_state == identifier_end:
                if self.debug:
                    logging.debug(f"重置刷新状态标识")
                self.refreshTimeTask_identifier = ""
                
        except Exception as e:
            logging.error(f"刷新任务时发生错误：{str(e)}")
            if self.debug:
                import traceback
                logging.debug(traceback.format_exc())
       
    #获取功能数组    
    def getFuncArray(self, modelArray):
        #待消费数组
        featureArray = []
        #当前待消费数组
        currentExpendArray=[]
        #过期任务数组
        historyArray=[]
        #遍历检查时间
        for item in modelArray:
            model : TimeTaskModel = item
            if model.enable:
                #是否现在时刻
                is_nowTime, nowTime = model.is_nowTime()
                #是否未来时刻
                is_featureTime = model.is_featureTime()
                #是否today
                is_today = model.is_today()
                #是否未来day
                is_featureDay = model.is_featureDay()
                
                if self.debug:
                    logging.debug(f"任务 {model.taskId} 检查结果：")
                    logging.debug(f"- 是否现在时刻: {is_nowTime}")
                    logging.debug(f"- 是否今天: {is_today}")
                    logging.debug(f"- 是否已消费: {model.is_today_consumed}")
                    logging.debug(f"- 是否cron任务: {model.isCron_time()}")
            
                #是否历史
                isHistory = True
                #由于一个model既可以是当前的任务，又可能是以后得任务，所以这里对一个model同时判定现在和未来的判定
                #是否现在时刻的任务
                if is_nowTime and is_today:
                    #精度为分钟，cron中消费本次任务
                    if model.isCron_time():
                        if nowTime in model.cron_today_times:
                            model.cron_today_times.remove(nowTime)
                            currentExpendArray.append(model)
                            isHistory = False
                            if self.debug:
                                logging.debug(f"任务 {model.taskId} 将被执行（cron任务）")
                        
                    #今天未被消费
                    elif not model.is_today_consumed:
                        currentExpendArray.append(model)
                        isHistory = False
                        model.is_today_consumed = True
                        if self.debug:
                            logging.debug(f"任务 {model.taskId} 将被执行（普通任务）")
                    else:
                        if self.debug:
                            logging.debug(f"任务 {model.taskId} 今天已被消费，跳过执行")
                
                #是否当前时刻后面待消费任务
                if (is_featureTime and is_today) or is_featureDay:
                    featureArray.append(model)
                    isHistory = False                     
                
                #存入历史数组
                if isHistory:
                    historyArray.append(model.get_formatItem())
            else:
                historyArray.append(model.get_formatItem())  
        
        return  historyArray, currentExpendArray, featureArray     
        
          
    #执行task
    def runTaskArray(self, modelArray):
        try:
            # Add deduplication check
            executed_tasks = set()
            for model in modelArray:
                if model.taskId not in executed_tasks:
                    executed_tasks.add(model.taskId)
                    self.runTaskItem(model)
                else:
                    print(f"Skipping duplicate task execution for ID: {model.taskId}")
        except Exception as e:
            print(f"执行定时任务，发生了错误：{e}")
            
                
    #执行task
    def runTaskItem(self, model: TimeTaskModel):
        try:
            # 获取当前时间，用于任务锁
            current_minute = arrow.now().format('YYYY-MM-DD HH:mm')
            task_lock_key = f"{model.taskId}_{current_minute}"
            
            if self.debug:
                logging.debug(f"准备执行任务 {model.taskId}")
                logging.debug(f"当前时间: {current_minute}")
                logging.debug(f"任务锁key: {task_lock_key}")
            
            # 检查任务锁
            if hasattr(self, '_task_locks'):
                if task_lock_key in self._task_locks:
                    if self.debug:
                        logging.debug(f"任务 {model.taskId} 在 {current_minute} 已经执行过，跳过")
                    return
            else:
                if self.debug:
                    logging.debug(f"初始化任务锁集合")
                self._task_locks = set()
            
            # 添加任务锁
            self._task_locks.add(task_lock_key)
            if self.debug:
                logging.debug(f"已添加任务锁")
            
            try:
                #非cron，置为已消费
                if not model.isCron_time():
                    if self.debug:
                        logging.debug(f"非cron任务，标记为已消费")
                    model.is_today_consumed = True
                    #置为消费
                    ExcelTool().write_columnValue_withTaskId_toExcel(model.taskId, 14, "1")
                
                if self.debug:
                    logging.debug(f"开始执行任务: {model.taskId}")
                    logging.debug(f"任务详情: {model.circleTimeStr} {model.timeStr} {model.eventStr}")
                
                #回调定时任务执行
                if self.timeTaskFunc:
                    self.timeTaskFunc(model)
                else:
                    logging.warning(f"警告：任务 {model.taskId} 的回调函数未设置")
                
                #任务消费
                if not model.is_featureDay():
                    if self.debug:
                        logging.debug(f"非未来任务，禁用该任务")
                    obj = ExcelTool()
                    obj.write_columnValue_withTaskId_toExcel(model.taskId , 2, "0")
                    #刷新数据
                    self.refreshDataFromExcel()
                    
            finally:
                # 任务执行完成后移除锁
                if task_lock_key in self._task_locks:
                    self._task_locks.remove(task_lock_key)
                    if self.debug:
                        logging.debug(f"已移除任务锁")
            
        except Exception as e:
            logging.error(f"执行定时任务时发生错误：{str(e)}")
            if self.debug:
                import traceback
                logging.debug(traceback.format_exc())
        
    #添加任务
    def addTask(self, taskModel: TimeTaskModel):
        taskList = ExcelTool().addItemToExcel(taskModel.get_formatItem())
        self.convetDataToModelArray(taskList)
        return taskModel.taskId   
    
    #model数组转换
    def convetDataToModelArray(self, dataArray):
        tempArray = []
        for item in dataArray:
            model = TimeTaskModel(item, None, False, True)
            tempArray.append(model)
        #赋值
        self.timeTasks = tempArray
        
    #是否目标时间      
    def is_targetTime(self, timeStr):
        tempTimeStr = timeStr
        #对比精准到分（忽略秒）
        current_time = arrow.now().format('HH:mm')
        
        #如果是分钟
        if tempTimeStr.count(":") == 1:
           tempTimeStr = tempTimeStr + ":00"
        
        #转为分钟时间
        task_time = arrow.get(tempTimeStr, "HH:mm:ss").format("HH:mm")
        tempValue = current_time == task_time
        return tempValue 

    def execute_task(self, task_id, task_info):
        """执行定时任务"""
        try:
            logger.info(f"[TimeTask] 开始执行定时任务，任务ID: {task_id}")
            logger.info(f"[TimeTask] 事件信息: {task_info}")
            
            # 添加任务锁，防止重复执行
            task_lock_key = f"task_lock_{task_id}"
            if self.task_locks.get(task_lock_key):
                logger.warning(f"[TimeTask] 任务 {task_id} 正在执行中，跳过本次执行")
                return
            
            self.task_locks[task_lock_key] = True
            try:
                # 执行任务
                if task_info.get('type') == 'clean_files':
                    days = task_info.get('days', 3)
                    clean_files = CleanFiles(task_info.get('path'))
                    clean_files.clean_expired_files(days)
                else:
                    # 其他类型的任务处理
                    pass
                
                logger.info(f"[TimeTask] 任务 {task_id} 执行完成")
            finally:
                # 确保任务锁被释放
                self.task_locks[task_lock_key] = False
                
        except Exception as e:
            logger.error(f"[TimeTask] 执行任务 {task_id} 出错: {str(e)}")
            
    def parse_time(self, time_str):
        """解析时间字符串，支持多种格式"""
        try:
            # 尝试多种时间格式
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y-%m-%d',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d %H:%M',
                '%Y/%m/%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(time_str, fmt)
                except ValueError:
                    continue
                    
            raise ValueError(f"无法解析时间格式: {time_str}")
            
        except Exception as e:
            logger.error(f"时间解析错误: {str(e)}")
            return None