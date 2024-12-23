# encoding:utf-8

import os
import sys
import arrow
import logging
import time
import threading
from typing import List
from plugins.timetask.Tool import ExcelTool
from plugins.timetask.Tool import TimeTaskModel
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
        
        # 初始化任务列表
        self.timeTasks = []  # 任务列表
        self.historyTasks = []  # 历史任务列表
        self.refreshTimeTask_identifier = ""  # 刷新任务标识符
        self.moveHistoryTask_identifier = ""  # 迁移历史任务标识符
        
        # 加载配置
        load_config()  # 确保先加载配置
        self.conf = conf()  # 保存配置对象
        self.debug = self.conf.get('debug', False)
        self.move_historyTask_time = self.conf.get('move_historyTask_time', '04:00:00')
        self.time_check_rate = self.conf.get('time_check_rate', 1)
        
        # 初始化任务列表
        try:
            all_tasks = ExcelTool().readExcel()
            if all_tasks:
                self.timeTasks = [TimeTaskModel(task, None, False, True) for task in all_tasks if self._is_valid_task(task)]
                print(f"[DEBUG] 成功加载 {len(self.timeTasks)} 个任务")
            else:
                print("[DEBUG] 没有找到任何任务")
        except Exception as e:
            print(f"[ERROR] 初始化任务列表时出错: {str(e)}")
            
        # 初始化任务状态
        try:
            self.initTaskStates()
        except Exception as e:
            print(f"[ERROR] 初始化任务状态时出错: {str(e)}")
        
        # 创建子线程
        t = threading.Thread(target=self.pingTimeTask_in_sub_thread)
        t.setDaemon(True) 
        t.start()
        
    def _is_valid_task(self, task):
        """检查任务是否有效"""
        try:
            model = TimeTaskModel(task, None, False, True)
            if not model.timeStr or not model.circleTimeStr:
                print(f"[DEBUG] 任务 {model.taskId} 时间格式无效，跳过")
                return False
            return True
        except Exception as e:
            print(f"[ERROR] 验证任务有效性时出错: {str(e)}")
            return False

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
        
        print(f"Debug mode is {'on' if self.debug else 'off'}")  
        
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
        """定时检查任务"""
        try:
            # 检查登录状态
            if self.isRelogin:
                print("[DEBUG] 系统重新登录中，跳过任务检查")
                return
                
            current_time = arrow.now()
            
            # 获取待执行任务
            modelArray = self.timeTasks
            historyArray, currentExpendArray, featureArray = self.getFuncArray(modelArray)
            
            # 处理历史任务
            if len(historyArray) > 0:
                for item in historyArray:
                    if item not in currentExpendArray and item not in featureArray and item not in self.historyTasks:
                        self.historyTasks.append(item)
                        print(f"[DEBUG] 添加历史任务: {item.taskId}")
            
            # 凌晨刷新任务状态
            if self.is_targetTime("00:00"):
                print("[DEBUG] 执行凌晨任务状态刷新")
                self.refresh_times(featureArray)
                # 重新初始化任务状态
                self.initTaskStates()
            
            # 迁移历史任务
            if self.is_targetTime(self.move_historyTask_time):
                print(f"[DEBUG] 执行历史任务迁移，时间: {self.move_historyTask_time}")
                self.moveTask_toHistory(self.historyTasks)
            
            # 无任务时直接返回
            if len(modelArray) <= 0:
                return
            
            # 更新任务数组
            timeTask_ids = '😄'.join(item.taskId for item in self.timeTasks)
            modelArray_ids = '😄'.join(item.taskId for item in modelArray)
            featureArray_ids = '😄'.join(item.taskId for item in featureArray)
            
            if timeTask_ids == modelArray_ids and timeTask_ids != featureArray_ids:
                self.timeTasks = featureArray
                print(f"[DEBUG] 更新任务数组")
                print(f"[DEBUG] 原任务: {timeTask_ids}")
                print(f"[DEBUG] 新任务: {featureArray_ids}")
            
            # 处理当前待执行任务
            if len(currentExpendArray) <= 0:
                if self.debug:
                    print("[DEBUG] 当前无待执行任务")
                return
                
            print(f"[DEBUG] 发现 {len(currentExpendArray)} 个待执行任务")
            
            # 任务锁检查
            current_minute = current_time.format('YYYY-MM-DD HH:mm')
            filtered_tasks = []
            
            for task in currentExpendArray:
                task_lock_key = f"{task.taskId}_{current_minute}"
                if task_lock_key in self._task_locks:
                    print(f"[DEBUG] 任务 {task.taskId} 在 {current_minute} 已执行，跳过")
                    continue
                # 先添加任务锁,再添加到待执行列表
                self._task_locks.add(task_lock_key)
                filtered_tasks.append(task)
            
            # 执行任务
            if filtered_tasks:
                print(f"[DEBUG] 开始执行 {len(filtered_tasks)} 个任务")
                self.runTaskArray(filtered_tasks)
            
            # 清理过期任务锁
            self._cleanTaskLocks()
        
        except Exception as e:
            print(f"[ERROR] 任务检查时发生错误: {str(e)}")
        
    def _cleanTaskLocks(self):
        """清理过期的任务锁"""
        try:
            current_time = arrow.now()
            old_count = len(self._task_locks)
            
            # 只保留最近30分钟的任务锁
            self._task_locks = {
                lock for lock in self._task_locks 
                if current_time.shift(minutes=-30).format('YYYY-MM-DD HH:mm') <= lock.split('_')[1]
            }
            
            new_count = len(self._task_locks)
            if old_count != new_count:
                print(f"[DEBUG] 清理了 {old_count - new_count} 个过期任务锁")
                
        except Exception as e:
            print(f"[ERROR] 清理任务锁时发生错误: {str(e)}")
            
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
        """刷新任务时间"""
        try:
            print("[DEBUG] 开始刷新任务时间")
            current_time = arrow.now()
            
            for item in modelArray:
                if not isinstance(item, TimeTaskModel):
                    continue
                    
                try:
                    # 重置任务状态
                    item.is_today_consumed = False
                    success = ExcelTool().write_columnValue_withTaskId_toExcel(item.taskId, 14, "0")
                    
                    if success:
                        print(f"[DEBUG] 重置任务 {item.taskId} 状态成功")
                    else:
                        print(f"[ERROR] 重置任务 {item.taskId} 状态失败")
                        
                    # 处理cron任务
                    if item.isCron_time():
                        from croniter import croniter
                        base = current_time.datetime
                        cron = croniter(item.circleTimeStr + " " + item.timeStr, base)
                        next_time = arrow.get(cron.get_next())
                        item.next_run_time = next_time
                        print(f"[DEBUG] 更新cron任务 {item.taskId} 下次执行时间: {next_time}")
                        
                except Exception as e:
                    print(f"[ERROR] 刷新任务 {item.taskId} 时间时出错: {str(e)}")
                    continue
                    
            print("[DEBUG] 任务时间刷新完成")
            
        except Exception as e:
            print(f"[ERROR] 刷新任务时间时出错: {str(e)}")

    #获取功能数组    
    def getFuncArray(self, modelArray):
        """获取任务数组"""
        try:
            historyArray = []  # 历史任务
            currentExpendArray = []  # 当前待消费任务
            featureArray = []  # 未来任务
            
            current_time = arrow.now()
            current_date = current_time.format('YYYY-MM-DD')
            current_time_str = current_time.format('HH:mm:ss')
            
            for item in modelArray:
                if not isinstance(item, TimeTaskModel):
                    continue
                    
                task_time = item.timeStr
                task_date = item.circleTimeStr
                
                # 处理cron表达式
                if item.isCron_time():
                    from croniter import croniter
                    try:
                        cron = croniter(task_date + " " + task_time)
                        next_time = arrow.get(cron.get_next())
                        # 如果下次执行时间在当前时间之后，加入未来任务
                        if next_time > current_time:
                            featureArray.append(item)
                        else:
                            currentExpendArray.append(item)
                    except Exception as e:
                        print(f"[ERROR] 处理cron表达式时出错: {str(e)}")
                        continue
                    continue
                
                # 处理普通日期任务
                try:
                    task_datetime = arrow.get(f"{task_date} {task_time}", "YYYY-MM-DD HH:mm:ss")
                    
                    # 如果任务时间在当前时间之前，加入历史任务
                    if task_datetime < current_time:
                        historyArray.append(item)
                    # 如果任务时间等于当前时间（忽略秒），加入当前任务
                    elif task_datetime.format('YYYY-MM-DD HH:mm') == current_time.format('YYYY-MM-DD HH:mm'):
                        currentExpendArray.append(item)
                    # 如果任务时间在当前时间之后，加入未来任务
                    else:
                        featureArray.append(item)
                except Exception as e:
                    print(f"[ERROR] 处理任务日期时出错: {str(e)}")
                    continue
            
            return historyArray, currentExpendArray, featureArray
            
        except Exception as e:
            print(f"[ERROR] 获取任务数组时出错: {str(e)}")
            return [], [], []
          
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
    def runTaskItem(self, item):
        """执行单个任务"""
        try:
            if not item or not isinstance(item, TimeTaskModel):
                print(f"[ERROR] 无效的任务对象")
                return False
                
            if not item.enable:
                print(f"[DEBUG] 任务 {item.taskId} 已禁用，跳过执行")
                return False
            
            current_time = arrow.now()
            current_minute = current_time.format('YYYY-MM-DD HH:mm')
            task_lock_key = f"{item.taskId}_{current_minute}"
            
            # 检查任务锁
            if task_lock_key in self._task_locks:
                print(f"[DEBUG] 任务 {item.taskId} 在 {current_minute} 已执行，跳过")
                return False
                
            # 添加任务锁
            self._task_locks.add(task_lock_key)
            print(f"[DEBUG] 添加任务锁: {task_lock_key}")
            
            try:
                # 执行任务
                print(f"[DEBUG] 开始执行任务: {item.taskId}")
                
                # 处理群发送
                if item.group_name:
                    group_list = self.get_group_list()
                    target_group = next((group for group in group_list if item.group_name in group['NickName']), None)
                    
                    if target_group:
                        print(f"[DEBUG] 找到目标群: {target_group['NickName']}")
                        self.send_to_group(target_group['UserName'], item.content)
                    else:
                        print(f"[ERROR] 未找到目标群: {item.group_name}")
                        return False
                else:
                    # 普通消息发送
                    self.send_to_user(item.user_id, item.content)
                
                print(f"[DEBUG] 任务 {item.taskId} 执行完成")
                return True
                
            except Exception as e:
                print(f"[ERROR] 执行任务 {item.taskId} 时出错: {str(e)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] 处理任务 {item.taskId if item else 'Unknown'} 时出错: {str(e)}")
            return False

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

    def initTaskStates(self):
        """初始化任务状态"""
        try:
            print("[DEBUG] 开始初始化任务状态")
            current_time = arrow.now()
            
            for task in self.timeTasks:
                if not isinstance(task, TimeTaskModel):
                    continue
                    
                try:
                    # 重置任务状态
                    task.is_today_consumed = False
                    
                    # 更新Excel中的状态
                    success = ExcelTool().write_columnValue_withTaskId_toExcel(task.taskId, 14, "0")
                    if success:
                        print(f"[DEBUG] 重置任务 {task.taskId} 状态成功")
                    else:
                        print(f"[ERROR] 重置任务 {task.taskId} 状态失败")
                    
                    # 处理cron任务
                    if task.isCron_time():
                        from croniter import croniter
                        base = current_time.datetime
                        cron = croniter(task.circleTimeStr + " " + task.timeStr, base)
                        next_time = arrow.get(cron.get_next())
                        task.next_run_time = next_time
                        print(f"[DEBUG] 设置cron任务 {task.taskId} 下次执行时间: {next_time}")
                        
                except Exception as e:
                    print(f"[ERROR] 初始化任务 {task.taskId} 状态时出错: {str(e)}")
                    continue
            
            print("[DEBUG] 任务状态初始化完成")
            
        except Exception as e:
            print(f"[ERROR] 初始化任务状态时出错: {str(e)}")