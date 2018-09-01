#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xlwt

TABLE_NAME = 'data-cas-sequential'

def write_excel(path):
    workbook = xlwt.Workbook(encoding='utf-8')
    data_sheet = workbook.add_sheet('demo')

    first_col = [TABLE_NAME, '64B', '512B', '1K', '2K', '4K', '16K', '64K', '128K']
    with open(TABLE_NAME, 'r') as f:
        lines = f.readlines()
        cnt = 0
        size = '0'
        time = 0
        total = []

        for line in lines:
            row = line.replace('\n','').split(' ')
            if (cnt == 0):
                data_sheet.write(cnt, 0, first_col[cnt])
                for i in range(0, (len(row)-1)//2):
                    data_sheet.write(cnt, i+1, row[i*2+1])
            if (row[0] != size):
                cnt += 1
                size = row[0]
                if total == []:
                    for i in range(2, len(row), 2):
                        total.append(float(row[i]))
                    time = 1
                else:
                    data_sheet.write(cnt-1, 0, first_col[cnt-1])
                    for i in range(0,len(total)):
                        data_sheet.write(cnt-1, i+1, float(total[i])/time)
                    total = []
                    for i in range(2, len(row), 2):
                        total.append(float(row[i]))
                    time = 1
            else:
                for i in range(0,len(total)):
                    total[i] += float(row[i*2+2])
                time += 1

    data_sheet.write(cnt, 0, first_col[cnt])
    for i in range(0,len(total)):
        data_sheet.write(cnt, i+1, float(total[i])/time)

    workbook.save(path)

if __name__ == '__main__':
    path = './' + TABLE_NAME + '.xls'
    write_excel(path)
    print('create table successfully!')