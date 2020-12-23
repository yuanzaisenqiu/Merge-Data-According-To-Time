import pandas as pd
import time
from os import chdir, listdir, getcwd, getpid
from csv import DictReader
from sys import argv
from multiprocessing import Process


# 计算时间增量
def calDeltaTime(next_timestamp, last_timestamp):
    seconds = int(next_timestamp - last_timestamp)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "{0:02d}:{1:02d}:{2:02d}".format(h, m, s)


def mergeCSV(path, location, camera_exist, display_exist):

    # 切换路径
    chdir(path)
    print(path, getpid())
    # chdir(r'C:\Users\Sunday\Desktop')

    # 一、处理allKeys数据
    # 从allKeys提取需要的列，填充
    allKeyData = pd.read_csv('smcAllkeys.csv', encoding='utf-8')
    allKeyDataTime = allKeyData.iloc[:, 0:1]
    allKeyDataTXXX = allKeyData.loc[:, 'TG0A':'TV3s']
    allKeyDataNeed = allKeyDataTime.join(allKeyDataTXXX).fillna(method='ffill')  # 用前一个数据代替缺失值 or pad

    # 在最后增加一个时间戳的列
    allKeyDataNeed['timestamp'] = allKeyDataNeed['DateTime'].apply(
        lambda x: time.mktime(time.strptime(x, '%m/%d/%Y %H:%M:%S.%f')))    # 12/15/2020 11:12:59.034

    # 在第二列增加一列时间增量列
    allKeyDataNeed.insert(1, 'deltatime', allKeyDataNeed['timestamp'].apply(
        lambda x: calDeltaTime(x, allKeyDataNeed.iloc[0, -1])))

    # 写出allKeys
    allKeyDataNeed.to_csv('allKeyNeed.csv', encoding='utf-8', index=False)

    # 二、处理smcPower和nitsLog
    smcPowerData = pd.read_csv('smcPower.csv', encoding='utf-8')
    nitsLogData = pd.read_csv('nitsLog.csv', encoding='utf-8')
    smcPowerData['timestamp'] = smcPowerData['Date/Time'].apply(
        lambda x: time.mktime(time.strptime(x, '%a %m/%d/%y %I:%M:%S %p')))  # Tue 12/15/20 11:13:03 AM
    nitsLogData['timestamp'] = nitsLogData['Date/Time'].apply(
        lambda x: time.mktime(time.strptime(x, '%a %b %d %H:%M:%S %Y')))    # Tue Dec 15 11:13:01 2020

    # 三、处理机台外部温度
    TCData = pd.read_csv('TC.csv', encoding='utf-8')

    # 截取TC的数据，范围是smcPower开始的前一分钟到结束的后一分钟
    TCStartTime = int(time.mktime(time.strptime(smcPowerData.iloc[0, 0], '%a %m/%d/%y %I:%M:%S %p')))
    TCEndTime = int(time.mktime(time.strptime(smcPowerData.iloc[-1, 0], '%a %m/%d/%y %I:%M:%S %p')))
    # 持续时间 （smcTime + 2）mins
    durationTime = TCEndTime - TCStartTime + 120
    myList = [x + TCStartTime - 60 for x in range(durationTime + 1)]
    # 新增一个timestamp的DataFrame
    TCTime1S = pd.DataFrame(myList, columns=['timestamp'])
    # 对TC.csv增加一个时间戳
    TCData['timestamp'] = TCData['Time'].apply(
        lambda x: time.mktime(time.strptime(x, '%Y/%m/%d %H:%M:%S')))
    # 以TCTime1S为基准合成并填充
    TCData1S = pd.merge(TCTime1S, TCData, how='left', on='timestamp').fillna(method='pad')

    # 获取机台所对应的ovenData
    if location == 1 or location == 2:
        ovenData = TCData1S.iloc[:, 83:87]
    elif location == 3 or location == 4:
        ovenData = TCData1S.iloc[:, 87:91]
    elif location == 5 or location == 6:
        ovenData = TCData1S.iloc[:, 91:95]
    else:
        ovenData = TCData1S.iloc[:, 95:99]

    # 环境温度Data
    roomAmbientData = TCData1S.iloc[:, 99:100]

    # 合并ovenData和roomAmbientData数据，并修改列名
    # DictReader会将第一行的内容（类标题）作为key值，第二行开始才是数据内容
    # TCMap.csv对应的是TC的数量，请注意
    # 
    with open(r'C:\Users\Sunday\Desktop\TCMap.csv') as f:
        TCName = [row["name"] for row in DictReader(f)]
    ovenAndAmbientName = ['T1', 'T2', 'S3', 'P4', 'ambient']
    ovenAndAmbientData = ovenData.join(roomAmbientData)
    ovenAndAmbientData.columns = ovenAndAmbientName

    # 处理机台的TC数据
    TCQty = len(TCName)
    startCol = (location - 1) * 10 + 3
    endCol = startCol + TCQty
    unitTCData = TCData1S.iloc[:, startCol:endCol]
    unitTCData.columns = TCName

    TCTime = TCData1S.iloc[:, 0:3]  # 0 1 2 loc # timestamp	Scan Time
    # finallyUnitTCData = pd.merge(TCTime, unitTCData, left_index=True, right_index=True)
    timeAndUnitTCData = TCTime.join(unitTCData)    # 时间戳+tc的数据
    finallyUnitTCData = timeAndUnitTCData.join(ovenAndAmbientData)  # 时间戳+tc的数据+oven数据+环境温度
    del finallyUnitTCData['Scan']
    finallyUnitTCData.to_csv('finallyUnitTCData.csv', index=False)

    # 四、开始合成 allKey.csv + nitsLog.csv + TC.csv
    weNeedData = pd.merge(allKeyDataNeed, smcPowerData, how='left', on='timestamp')
    del weNeedData['Date/Time']
    weNeedData = pd.merge(weNeedData, nitsLogData, how='left', on='timestamp')
    del weNeedData['Date/Time']

    # 查看是否还有其他csv需要合成（cameraFPS.csv、Display_FPS_101021.csv）
    # cameraFPS.csv
    if camera_exist == 1:
        cameraFPSData = pd.read_csv('cameraFPS.csv', encoding='utf-8')
        cameraFPSData['timestamp'] = cameraFPSData['Date/Time'].apply(
            lambda x: time.mktime(time.strptime(x, '%a %b %d %H:%M:%S %Y')))    # Tue Dec 15 11:13:01 2020
        weNeedData = pd.merge(weNeedData, cameraFPSData, how='left', on='timestamp')
        del weNeedData['Date/Time']
    # Display_FPS.csv
    if display_exist == 1:
        displayFPSData = pd.read_csv('Display_FPS.csv', encoding='utf-8')
        displayFPSData['timestamp'] = displayFPSData['Date/Time'].apply(
            lambda x: time.mktime(time.strptime(x, '%Y-%m-%d  %H:%M:%S.%f%z')))    # 2020-09-01 10:10:25.577494+0800
        weNeedData = pd.merge(weNeedData, displayFPSData, how='left', on='timestamp')
        del weNeedData['Date/Time']

    # TC最后合成
    weNeedData = pd.merge(weNeedData, finallyUnitTCData, how='left', on='timestamp')
    del weNeedData['Time']
    del weNeedData['timestamp']

    # 过滤成5s一次的数据
    weNeedData = weNeedData.loc[weNeedData.index % 5 == 0]
    weNeedData.to_csv('weNeedData.csv', index=False)


def main():
    # 设置预览为10
    pd.options.display.max_rows = 10
    start = time.time()
    cameraFPSMerge = 0
    displayFPSMerge = 0
    print(getpid())
    chdir(r'C:\Users\Sunday\Desktop\D63_P1U_TFS-N+ShaderBenchG13')
    processList = []
    unitDirList = listdir()
    for unitDir in unitDirList:
        unitLocation = int(unitDir.split('-')[0])    # 5-123b  ==> 5
        p = Process(target=mergeCSV, args=(unitDir, unitLocation, cameraFPSMerge, displayFPSMerge, ))
        processList.append(p)
    # print(processList)
    for p in processList:
        p.start()
    for p in processList:
        p.join()

    end = time.time()
    print('总共耗费了%.2f秒.' % (end - start))
    '''
    # shell程序还需判断所有的机台名称必须是[1-8]-[0-9]*[a-z]
    # 让shell程序判断是否有cameraFPS.csv和Display_FPS_101021.csv（修改为Display_FPS.csv），并且非空，再传入参数 0 1
    # mergeCSV()函数需要传递的参数 
    （1）文件夹名称eg:1-152w
    （2）分离1-152w，把机台位置1取出来
    （3）cameraFPS.csv 0/1
    （4）Display_FPS_101021.csv 0/1
    
    print(argv[0])  # sys.argv[0] 类似于shell中的$0,但不是脚本名称，而是脚本的路径
    print(argv[1])  # sys.argv[1] 表示传入的第一个参数，既 hello
    '''


if __name__ == '__main__':
    main()
